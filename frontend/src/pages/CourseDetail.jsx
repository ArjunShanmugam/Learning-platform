import React, { useEffect, useState, useContext, useRef } from "react";
import { useParams, useNavigate } from "react-router-dom";
import api from "../utils/api";
import { AuthContext } from "../context/AuthContext";

export default function CourseDetail() {
  const { id } = useParams();
  const { user } = useContext(AuthContext);
  const navigate = useNavigate();
  const userId = user ? user.id : null;

  const [course, setCourse] = useState(null);
  const [userProfile, setUserProfile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [completing, setCompleting] = useState(false);
  const [completed, setCompleted] = useState(false);
  const [progressionMessage, setProgressionMessage] = useState("");
  const [isAlreadyCompleted, setIsAlreadyCompleted] = useState(false);
  const [levelRestricted, setLevelRestricted] = useState(false);
  const startTs = useRef(null);

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const res = await api.get(`/courses`);
        const found = (res.data || []).find(c => String(c.id) === String(id));
        if (mounted) setCourse(found || null);
        
        // Check level restriction
        if (found && userId) {
          try {
            const profileRes = await api.get(`/auth/users/${userId}/profile`);
            if (mounted && profileRes.data) {
              setUserProfile(profileRes.data);
              
              // Check if course difficulty is too high
              const skillLevels = ["Beginner", "Mid", "Expert"];
              const userLevel = skillLevels.indexOf(profileRes.data.skill_level);
              const courseLevel = skillLevels.indexOf(found.difficulty);
              
              // User can take course if: same level OR one level above
              if (courseLevel > userLevel + 1) {
                setLevelRestricted(true);
              }
            }
          } catch (e) {
            console.log("Could not fetch user profile");
          }
        }
      } catch (err) {
        console.error(err);
      } finally {
        if (mounted) setLoading(false);
      }
    }
    load();

    (async () => {
      if (userId) {
        try {
          // Log view interaction for recommendations
          // Try the new interaction endpoint, but don't fail if not authenticated
          try {
            await api.post(`/search/log-interaction?course_id=${Number(id)}&interaction_type=view`);
          } catch (interactionError) {
            // If interaction logging fails (401, 403, etc), continue without it
            // This is non-critical for the core experience
            if (interactionError.response?.status !== 401 && interactionError.response?.status !== 403) {
              console.warn("Interaction logging issue:", interactionError.message);
            }
          }
          
          // Also log click for backwards compatibility
          await api.post("/logs/click", { user_id: userId, course_id: Number(id), event: "open" });
        } catch (e) {
          console.error("log view/click", e);
        }
      }
      startTs.current = Date.now();
    })();

    return () => {
      const dt = startTs.current ? (Date.now() - startTs.current) / 1000.0 : 0;
      if (userId && dt >= 1) {
        // Log view interaction for recommendations (duration tracking)
        api.post(`/search/log-interaction?course_id=${Number(id)}&interaction_type=view`)
           .catch(e => {
             // Graceful error handling - not critical
             if (e.response?.status !== 401 && e.response?.status !== 403) {
               console.error("interaction log error", e);
             }
           });
      }
      mounted = false;
    };
  }, [id, userId]);

  const handleComplete = async () => {
    setCompleting(true);
    try {
      // Log completion interaction for recommendations
      try {
        await api.post(`/search/log-interaction?course_id=${course.id}&interaction_type=complete`);
      } catch (interactionError) {
        // If interaction logging fails, continue with completion anyway
        if (interactionError.response?.status !== 401 && interactionError.response?.status !== 403) {
          console.warn("Interaction logging issue:", interactionError.message);
        }
      }
      
      const res = await api.post("/logs/complete", { user_id: userId || 1, course_id: course.id });
      
      // Check if already completed
      if (res.data?.status === "already_marked") {
        setIsAlreadyCompleted(true);
        setProgressionMessage("✓ Course already marked as complete!");
        setCompleting(false);
        return;
      }

      // Mark as completed successfully
      setCompleted(true);
      setProgressionMessage("✅ Course marked as complete!");

      // Check skill progression
      try {
        const progressRes = await api.post("/skills/check-progression", {});
        if (progressRes.data?.status === "progressed") {
          setProgressionMessage(
            `✅ Course marked as complete!\n🎉 ${progressRes.data.message}`
          );
        }
      } catch (progressErr) {
        console.error("Skill progression check error:", progressErr);
        // Still show completion message even if progression check fails
      }

      setCompleting(false);
    } catch (e) {
      console.error("Completion error:", e);
      setProgressionMessage("❌ Failed to mark complete");
      setCompleting(false);
    }
  };

  if (loading) {
    return (
      <div className="w-full">
        <div className="skeleton h-screen"></div>
      </div>
    );
  }

  if (!course) {
    return (
      <div className="w-full flex items-center justify-center min-h-screen px-4">
        <div className="text-center">
          <svg className="w-24 h-24 mx-auto text-slate-600 mb-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4v2m0 0v2m0-6h4m-6 0H6" />
          </svg>
          <h1 className="text-3xl font-bold mb-2">Course Not Found</h1>
          <p className="text-slate-400 mb-8">The course you're looking for doesn't exist</p>
          <button
            onClick={() => navigate("/")}
            className="btn-primary px-6 py-3"
          >
            Back to Home
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="w-full">
      {/* Course Hero */}
      <section className="relative py-16 bg-gradient-to-r from-indigo-600/20 via-pink-600/10 to-orange-600/10 border-b border-slate-700 w-full px-4">
        <div className="max-w-4xl mx-auto">
          <button
            onClick={() => navigate(-1)}
            className="mb-8 flex items-center gap-2 text-indigo-400 hover:text-indigo-300 transition-colors"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
            Back
          </button>

          <div className="animate-fade-in">
            <div className="flex gap-3 mb-4">
              <span className="inline-block px-3 py-1 bg-gradient-to-r from-indigo-600 to-pink-600 text-white text-xs font-semibold rounded-full">
                {course.difficulty || "Intermediate"}
              </span>
              {course.career_path && (
                <span className="inline-block px-3 py-1 bg-slate-700 text-slate-200 text-xs font-semibold rounded-full">
                  {course.career_path}
                </span>
              )}
              {userProfile && (
                <span className="inline-block px-3 py-1 bg-amber-700/50 text-amber-200 text-xs font-semibold rounded-full">
                  Your Level: {userProfile.skill_level}
                </span>
              )}
            </div>

            {levelRestricted && (
              <div className="mb-6 p-4 bg-amber-500/20 border border-amber-500/50 rounded-lg">
                <p className="text-amber-200">
                  ⚠️ This course is too advanced for your current level. Complete more {course.difficulty === "Expert" ? "Mid" : "Beginner"} level courses first!
                </p>
              </div>
            )}

            <h1 className="text-5xl font-bold mb-6 leading-tight">
              {course.title}
            </h1>

            <p className="text-xl text-slate-300 mb-8">
              {course.description || "Master this skill with comprehensive lessons and hands-on projects"}
            </p>

            <div className="flex flex-col sm:flex-row gap-4">
              <button
                onClick={handleComplete}
                disabled={completing || completed || isAlreadyCompleted || levelRestricted}
                className="btn-primary px-8 py-3 text-lg font-semibold flex items-center justify-center gap-2 group disabled:opacity-70 disabled:cursor-not-allowed"
                title={levelRestricted ? "You must complete prerequisite courses first" : ""}
              >
                {completing ? (
                  <>
                    <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                    Completing...
                  </>
                ) : completed || isAlreadyCompleted ? (
                  <>
                    <svg className="w-5 h-5 text-green-400" fill="currentColor" viewBox="0 0 24 24">
                      <path d="M5 13l4 4L19 7" />
                    </svg>
                    Completed ✓
                  </>
                ) : (
                  <>
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                    </svg>
                    Mark as Complete
                  </>
                )}
              </button>

              {progressionMessage && (
                <div className="flex items-center gap-3 px-6 py-3 rounded-lg bg-green-500/10 border border-green-500/30">
                  <svg className="w-5 h-5 text-green-400 flex-shrink-0" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M5 13l4 4L19 7" />
                  </svg>
                  <span className="text-green-300 font-semibold whitespace-pre-wrap">{progressionMessage}</span>
                </div>
              )}

              <button className="px-8 py-3 rounded-lg border-2 border-slate-600 hover:border-indigo-500 font-semibold text-slate-200 transition-all duration-300 hover:bg-slate-700/30">
                <svg className="w-5 h-5 inline mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
                </svg>
                Save Course
              </button>
            </div>
          </div>
        </div>
      </section>

      {/* Course Content */}
      <section className="py-16 w-full px-4">
        <div className="max-w-4xl mx-auto">
          <div className="grid grid-cols-3 gap-6 mb-16 md:mb-20">
            {[
              { icon: "⏱", title: "4-6 Hours", desc: "Total duration" },
              { icon: "📚", title: "12 Lessons", desc: "Comprehensive content" },
              { icon: "👥", title: "5.2K", desc: "Students enrolled" }
            ].map((stat, i) => (
              <div key={i} className="glass rounded-lg p-6 text-center hover:border-indigo-500 transition-colors">
                <div className="text-3xl mb-2">{stat.icon}</div>
                <h3 className="font-bold text-lg mb-1">{stat.title}</h3>
                <p className="text-slate-400 text-sm">{stat.desc}</p>
              </div>
            ))}
          </div>

          {/* Curriculum */}
          <div className="mb-12">
            <h2 className="text-3xl font-bold mb-6">What You'll Learn</h2>
            <div className="space-y-3">
              {[
                "Master core concepts and fundamentals",
                "Build real-world projects from scratch",
                "Learn best practices and industry standards",
                "Get hands-on experience with tools and frameworks",
                "Understand how to optimize and scale",
                "Prepare for technical interviews"
              ].map((item, i) => (
                <div key={i} className="flex gap-3 items-start p-4 glass rounded-lg group hover:border-indigo-500 transition-colors">
                  <div className="w-6 h-6 rounded-full bg-indigo-500 flex items-center justify-center flex-shrink-0 mt-0.5">
                    <svg className="w-4 h-4 text-white" fill="currentColor" viewBox="0 0 24 24">
                      <path d="M5 13l4 4L19 7" />
                    </svg>
                  </div>
                  <span className="text-slate-300 group-hover:text-slate-100 transition-colors">{item}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Instructor */}
          <div className="glass rounded-lg p-8">
            <h2 className="text-2xl font-bold mb-6">About the Instructor</h2>
            <div className="flex gap-6 items-center">
              <div className="w-20 h-20 rounded-full bg-gradient-to-br from-indigo-500 to-pink-500 flex items-center justify-center flex-shrink-0">
                <svg className="w-10 h-10 text-white" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z" />
                </svg>
              </div>
              <div>
                <h3 className="font-bold text-xl mb-1">Expert Instructor</h3>
                <p className="text-slate-400 text-sm mb-3">
                  Certified professional with 10+ years of industry experience
                </p>
                <button className="text-indigo-400 hover:text-indigo-300 text-sm font-semibold transition-colors">
                  View Profile →
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

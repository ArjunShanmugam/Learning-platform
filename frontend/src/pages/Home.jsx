import React from "react";
import { Link } from "react-router-dom";
import Recommendations from "../components/Recommendations";
import api from "../utils/api";
import { AuthContext } from "../context/AuthContext";

export default function Home() {
  const { user } = React.useContext(AuthContext);
  const [featured, setFeatured] = React.useState([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    let mounted = true;
    setLoading(true);
    const params = { limit: 26 };
    if (user && user.id) params.user_id = user.id;
    
    api.get("/courses", { params })
      .then(res => {
        if (mounted) {
          setFeatured(res.data || []);
          setLoading(false);
        }
      })
      .catch(err => {
        console.error("featured load", err);
        if (mounted) setLoading(false);
      });
    return () => mounted = false;
  }, [user]);

  return (
    <div className="w-full">
      {/* Hero Section */}
      <section className="relative py-24 md:py-40 overflow-hidden w-full">
        <div className="absolute inset-0 bg-gradient-to-r from-indigo-600/15 via-purple-600/10 to-pink-600/15 blur-3xl"></div>
        <div className="relative w-full px-4">
          <div className="max-w-4xl mx-auto text-center animate-fade-in">
            <h1 className="text-5xl md:text-7xl font-900 mb-8 leading-tight">
              <span className="text-gradient">Master Any Skill</span>
              <br />
              <span className="text-slate-100">At Your Own Pace</span>
            </h1>
            <p className="text-lg md:text-xl text-slate-300 mb-10 max-w-2xl mx-auto leading-relaxed font-500">
              Learn from industry experts. Access world-class courses. Build your future with cutting-edge skills.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link to="/search" className="btn-primary text-base px-8 py-4 inline-flex items-center justify-center gap-2 group font-600">
                Explore Courses
                <svg className="w-5 h-5 group-hover:translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                </svg>
              </Link>
              <button className="px-8 py-4 rounded-lg border-2 border-indigo-500 text-indigo-300 hover:bg-indigo-500/10 hover:border-indigo-400 transition-all duration-300 font-600">
                Learn More
              </button>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-4 md:gap-8 mt-20 pt-16 border-t border-slate-700/50">
              <div className="animate-fade-in" style={{ animationDelay: '0.1s' }}>
                <div className="text-4xl md:text-5xl font-900 text-gradient mb-2">500+</div>
                <p className="text-sm md:text-base text-slate-400 font-500">Courses</p>
              </div>
              <div className="animate-fade-in" style={{ animationDelay: '0.2s' }}>
                <div className="text-4xl md:text-5xl font-900 text-gradient mb-2">50K+</div>
                <p className="text-sm md:text-base text-slate-400 font-500">Students</p>
              </div>
              <div className="animate-fade-in" style={{ animationDelay: '0.3s' }}>
                <div className="text-4xl md:text-5xl font-900 text-gradient mb-2">95%</div>
                <p className="text-sm md:text-base text-slate-400 font-500">Completion</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Recommendations Section */}
      <section className="py-20 md:py-28 w-full px-4">
        <div className="max-w-7xl mx-auto">
          <div className="mb-14">
            <h2 className="text-4xl md:text-5xl font-900 mb-4">
              <span className="text-gradient">Recommended For You</span>
            </h2>
            <p className="text-slate-400 text-lg font-500">Personalized learning paths based on your interests</p>
          </div>
          <Recommendations limit={9} />
        </div>
      </section>

      {/* Featured Courses Section */}
      <section className="py-20 md:py-28 w-full px-4">
        <div className="max-w-7xl mx-auto">
          <div className="mb-14">
            <h2 className="text-4xl md:text-5xl font-900 mb-4">
              <span className="text-gradient">Featured Courses</span>
            </h2>
            <p className="text-slate-400 text-lg font-500">Trending and top-rated courses</p>
          </div>

          {loading ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="card h-80 skeleton rounded-xl"></div>
              ))}
            </div>
          ) : featured.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {featured.map((course, idx) => (
                <Link 
                  key={course.id} 
                  to={`/courses/${course.id}`}
                  className="card group overflow-hidden animate-fade-in border border-slate-700/50 hover:border-indigo-500/50 p-0"
                  style={{ animationDelay: `${idx * 0.1}s` }}
                >
                  {/* Course Image Placeholder */}
                  <div className="h-56 bg-gradient-to-br from-indigo-600 via-purple-600 to-pink-600 relative overflow-hidden">
                    <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-300"></div>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <svg className="w-20 h-20 text-white/30 group-hover:scale-110 transition-transform duration-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 6.253v13m0-13C6.5 6.253 2 10.998 2 17s4.5 10.747 10 10.747c5.5 0 10-4.998 10-10.747S17.5 6.253 12 6.253z" />
                      </svg>
                    </div>
                    <div className="absolute top-4 left-4">
                      <span className="inline-block px-4 py-1.5 bg-indigo-600/90 text-white text-xs font-700 rounded-full backdrop-blur-sm">
                        Featured
                      </span>
                    </div>
                  </div>

                  {/* Course Info */}
                  <div className="p-6">
                    <h3 className="font-700 text-lg mb-3 group-hover:text-indigo-300 transition-colors line-clamp-2">
                      {course.title}
                    </h3>
                    <p className="text-sm text-slate-400 line-clamp-2 mb-4 font-500">
                      {course.description || "Master this skill with comprehensive lessons"}
                    </p>

                    {/* Footer */}
                    <div className="flex items-center justify-between pt-4 border-t border-slate-700/50">
                      <span className="text-xs font-700 text-indigo-300">
                        {course.difficulty || "Intermediate"}
                      </span>
                      <div className="flex gap-1">
                        {[...Array(5)].map((_, i) => (
                          <svg key={i} className="w-4 h-4 fill-yellow-400" viewBox="0 0 24 24">
                            <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
                          </svg>
                        ))}
                      </div>
                    </div>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <div className="text-center py-16">
              <p className="text-slate-400 text-lg font-500">No courses available yet</p>
            </div>
          )}
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 md:py-28 relative overflow-hidden w-full px-4 mb-8">
        <div className="absolute inset-0 bg-gradient-to-r from-indigo-600/12 via-purple-600/8 to-pink-600/12 blur-3xl"></div>
        <div className="relative max-w-3xl mx-auto">
          <div className="glass-strong rounded-2xl p-12 md:p-16 text-center border border-indigo-500/30">
            <h2 className="text-4xl md:text-5xl font-900 mb-4">Ready to Start Learning?</h2>
            <p className="text-slate-300 text-lg mb-8 font-500">
              Join thousands of students already learning on LearningHub
            </p>
            <Link to="/search" className="btn-primary px-8 py-4 inline-block font-600">
              Browse Courses Now
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}

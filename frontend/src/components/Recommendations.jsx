import React, { useEffect, useState, useContext } from "react";
import { useNavigate } from "react-router-dom";
import api from "../utils/api";
import { AuthContext } from "../context/AuthContext";

export default function Recommendations({ limit = 10 }) {
  const { user } = useContext(AuthContext);
  const navigate = useNavigate();
  const userId = user ? user.id : null;

  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const controller = new AbortController();
    let mounted = true;

    async function fetchRecs() {
      setLoading(true);
      setError(null);
      try {
        const params = {};
        if (userId) params.user_id = userId;
        if (limit) params.limit = limit;

        const res = await api.get("/recommendations/home", {
          params,
          signal: controller.signal,
        });

        if (!mounted) return;
        // Map course_id to id and reason to reasons array
        const mappedItems = Array.isArray(res.data) ? res.data.map(item => ({
          ...item,
          id: item.course_id,
          reasons: item.reason ? [item.reason] : []
        })) : [];
        setItems(mappedItems);
      } catch (err) {
        if (err.name === "CanceledError" || err.message === "canceled") {
          return;
        }
        console.error("Recommendations fetch error:", err);
        if (mounted) setError("Failed to load recommendations");
      } finally {
        if (mounted) setLoading(false);
      }
    }

    fetchRecs();
    return () => {
      mounted = false;
      controller.abort();
    };
  }, [userId, limit]);

  const handleOpen = async (courseId) => {
    try {
      await api.post("/logs/start", {
        user_id: userId,
        course_id: courseId,
      });

      await api.post("/logs/click", {
        user_id: userId,
        course_id: courseId,
        event: "open",
      });

      navigate(`/courses/${courseId}`);
    } catch (e) {
      console.error("open/start log error", e);
    }
  };

  const handleComplete = async (courseId) => {
    const removedItem = items.find(c => c.id === courseId);
    setItems((prev) => prev.filter((c) => c.id !== courseId));
    try {
      await api.post("/logs/complete", {
        user_id: userId,
        course_id: courseId,
      });
    } catch (e) {
      console.error("complete error", e);
      if (removedItem) {
        setItems(prev => [...prev, removedItem]);
      }
    }
  };

  if (loading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {[...Array(6)].map((_, i) => (
          <div key={`rec-skeleton-${i}`} className="card h-80 skeleton rounded-xl"></div>
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 bg-red-500/20 border border-red-500/50 rounded-xl">
        <p className="text-red-300">{error}</p>
      </div>
    );
  }

  if (!items.length) {
    return (
      <div className="text-center py-12">
        <svg className="w-16 h-16 mx-auto text-slate-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9 5h.01" />
        </svg>
        <p className="text-slate-400 text-lg">No recommendations found</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {items.map((item, idx) => (
        <div
          key={item.id}
          className="card group animate-fade-in"
          style={{ animationDelay: `${idx * 0.05}s` }}
        >
          {/* Course Header */}
          <div className="p-6 pb-4 border-b border-slate-700">
            <div className="flex items-start justify-between gap-4 mb-3">
              <h3 className="font-bold text-lg text-slate-100 group-hover:text-indigo-400 transition-colors line-clamp-2 flex-1">
                {item.title}
              </h3>
              {item.score && (
                <div className="flex-shrink-0 bg-indigo-500/20 px-3 py-1 rounded-lg border border-indigo-500/50">
                  <span className="text-sm font-semibold text-indigo-400">{Math.round(item.score * 10)}%</span>
                </div>
              )}
            </div>

            <div className="flex items-center gap-2">
              <span className="inline-block px-2 py-1 bg-gradient-to-r from-indigo-600 to-pink-600 text-white text-xs font-semibold rounded-full">
                {item.difficulty || "Intermediate"}
              </span>
              {item.career_path && (
                <span className="text-xs text-slate-400 bg-slate-700/50 px-2 py-1 rounded">
                  {item.career_path}
                </span>
              )}
            </div>
          </div>

          {/* Course Description */}
          <div className="p-6">
            <p className="text-slate-300 text-sm line-clamp-3 mb-4">
              {item.description || "Master this skill with comprehensive lessons"}
            </p>

            {/* Reasons */}
            {item.reasons && item.reasons.length > 0 && (
              <div className="mb-4 space-y-2">
                <p className="text-xs font-semibold text-slate-400 uppercase">Why recommended</p>
                <div className="flex flex-wrap gap-2">
                  {item.reasons.slice(0, 2).map((reason, i) => (
                    <span
                      key={i}
                      className="text-xs bg-indigo-500/20 text-indigo-300 px-2 py-1 rounded-full border border-indigo-500/30"
                    >
                      {reason}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Rating */}
            <div className="flex items-center gap-1 mb-4">
              {[...Array(5)].map((_, i) => (
                <svg key={i} className="w-4 h-4 fill-yellow-400" viewBox="0 0 24 24">
                  <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
                </svg>
              ))}
              <span className="text-xs text-slate-400 ml-2">(4.8/5)</span>
            </div>
          </div>

          {/* Actions */}
          <div className="px-6 pb-6 flex gap-3">
            <button
              onClick={() => handleOpen(item.id)}
              className="flex-1 btn-primary text-sm py-2 flex items-center justify-center gap-2 group"
            >
              <svg className="w-4 h-4 group-hover:translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
              </svg>
              Learn
            </button>

            <button
              onClick={() => handleComplete(item.id)}
              className="flex-1 px-4 py-2 rounded-lg border-2 border-slate-600 hover:border-green-500 text-slate-300 hover:text-green-400 transition-all duration-300 font-semibold text-sm flex items-center justify-center gap-2"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              Done
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

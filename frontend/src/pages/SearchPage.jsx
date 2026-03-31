import React, { useState, useContext } from "react";
import { Link } from "react-router-dom";
import api from "../utils/api";
import { AuthContext } from "../context/AuthContext";

export default function SearchPage() {
  const [q, setQ] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const { user } = useContext(AuthContext);
  const userId = user ? user.id : null;

  const doSearch = async (e) => {
    e.preventDefault();
    if (!q.trim()) return;

    setLoading(true);
    setSearched(true);
    try {
      // Log the search
      await api.post("/logs/search", { user_id: userId, query: q });
      
      // Use semantic search endpoint
      const res = await api.post("/search/semantic", { 
        q: q.trim(),
        user_id: userId
      });
      
      setResults(res.data || []);
    } catch (err) {
      console.error("Search error:", err);
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="w-full">
      {/* Search Header */}
      <section className="relative py-16 bg-gradient-to-r from-indigo-600/20 via-pink-600/10 to-orange-600/10 w-full px-4">
        <div className="max-w-2xl mx-auto animate-fade-in">
          <h1 className="text-4xl font-bold mb-4 text-center">
            <span className="text-gradient">Explore Courses</span>
          </h1>
          <p className="text-slate-300 text-center text-lg mb-8">
            Search from thousands of courses to find the perfect one for you
          </p>

          {/* Search Form */}
          <form onSubmit={doSearch} className="flex gap-3">
            <div className="flex-1 relative">
              <input
                type="text"
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search by course name, skill, or instructor..."
                className="input-modern w-full"
              />
              <svg className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <button type="submit" className="btn-primary px-8 py-3 flex items-center gap-2 whitespace-nowrap">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
              </svg>
              Search
            </button>
          </form>
        </div>
      </section>

      {/* Results Section */}
      <section className="py-16 w-full px-4">
        <div className="max-w-7xl mx-auto">
          {!searched ? (
            <div className="text-center py-12">
              <svg className="w-24 h-24 mx-auto text-slate-600 mb-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <h2 className="text-2xl font-bold text-slate-300 mb-2">Start Searching</h2>
              <p className="text-slate-400">Enter keywords to find courses that match your interests</p>
            </div>
          ) : loading ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {[...Array(6)].map((_, i) => (
                <div key={`skeleton-${i}`} className="card h-80 skeleton rounded-xl"></div>
              ))}
            </div>
          ) : results.length > 0 ? (
            <div>
              <div className="mb-8">
                <p className="text-slate-400 text-lg">
                  Found <span className="font-bold text-indigo-400">{results.length}</span> courses matching "<span className="font-bold">{q}</span>"
                </p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {results.map((course, idx) => (
                  <Link
                    key={`course-${course.course_id || idx}`}
                    to={`/courses/${course.course_id}`}
                    className="card group animate-fade-in"
                    style={{ animationDelay: `${idx * 0.05}s` }}
                  >
                    {/* Course Image */}
                    <div className="h-48 bg-gradient-to-br from-indigo-600 to-pink-600 relative overflow-hidden">
                      <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-300"></div>
                      <div className="absolute inset-0 flex items-center justify-center">
                        <svg className="w-16 h-16 text-white/40 group-hover:scale-110 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 6.253v13m0-13C6.5 6.253 2 10.998 2 17s4.5 10.747 10 10.747c5.5 0 10-4.998 10-10.747S17.5 6.253 12 6.253z" />
                        </svg>
                      </div>
                    </div>

                    {/* Course Info */}
                    <div className="p-6">
                      <h3 className="font-bold text-lg mb-2 group-hover:text-indigo-400 transition-colors line-clamp-2">
                        {course.title}
                      </h3>
                      <p className="text-sm text-slate-400 line-clamp-2 mb-4">
                        {course.description}
                      </p>

                      <div className="flex items-center justify-between pt-4 border-t border-slate-700">
                        <span className="text-xs font-semibold text-indigo-400">
                          {course.difficulty || "Intermediate"}
                        </span>
                        {course.career_path && (
                          <span className="text-xs text-slate-500 bg-slate-700/50 px-2 py-1 rounded">
                            {course.career_path}
                          </span>
                        )}
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            </div>
          ) : (
            <div className="text-center py-12">
              <svg className="w-24 h-24 mx-auto text-slate-600 mb-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <h2 className="text-2xl font-bold text-slate-300 mb-2">No Results Found</h2>
              <p className="text-slate-400 mb-6">Try searching with different keywords</p>
              <button
                onClick={() => { setQ(""); setSearched(false); setResults([]); }}
                className="btn-primary px-6 py-2"
              >
                Try Another Search
              </button>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}

import React, { useState } from "react";
import api from "../utils/api";

export default function AdminUpload() {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [difficulty, setDifficulty] = useState("Beginner");
  const [career, setCareer] = useState("");
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState(null);

  const submit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setSuccess(false);
    setError(null);
    try {
      const res = await api.post("/courses/admin", {
        title, description, difficulty, career_path: career
      });
      setSuccess(true);
      setTitle("");
      setDescription("");
      setCareer("");
      setDifficulty("Beginner");
      setTimeout(() => setSuccess(false), 3000);
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.detail || err.message || "Failed to create course");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="w-full">
      {/* Header */}
      <section className="border-b border-slate-700 py-12 w-full px-4">
        <div className="max-w-2xl mx-auto">
          <h1 className="text-4xl font-bold mb-3">
            <span className="text-gradient">Create New Course</span>
          </h1>
          <p className="text-slate-400 text-lg">
            Share your knowledge by creating a new course for the community
          </p>
        </div>
      </section>

      {/* Form */}
      <section className="py-12 w-full px-4">
        <div className="max-w-2xl mx-auto">
          <div className="glass rounded-2xl p-8 md:p-12">
            {success && (
              <div className="mb-6 p-4 bg-green-500/20 border border-green-500/50 rounded-lg flex gap-3 animate-fade-in">
                <svg className="w-5 h-5 text-green-400 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
                <div>
                  <h3 className="font-semibold text-green-300">Course Created Successfully!</h3>
                  <p className="text-green-200 text-sm">Your course is now live and available to students</p>
                </div>
              </div>
            )}

            {error && (
              <div className="mb-6 p-4 bg-red-500/20 border border-red-500/50 rounded-lg flex gap-3 animate-fade-in">
                <svg className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                </svg>
                <div>
                  <h3 className="font-semibold text-red-300">Error</h3>
                  <p className="text-red-200 text-sm">{error}</p>
                </div>
              </div>
            )}

            <form onSubmit={submit} className="space-y-6">
              {/* Title */}
              <div>
                <label className="block text-sm font-semibold mb-2 text-slate-300">Course Title *</label>
                <input
                  type="text"
                  value={title}
                  onChange={(e) => setTitle(e.target.value)}
                  placeholder="e.g., Advanced React Patterns"
                  className="input-modern w-full"
                  required
                />
                <p className="text-xs text-slate-400 mt-1">Make it clear, descriptive, and searchable</p>
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-semibold mb-2 text-slate-300">Course Description *</label>
                <textarea
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Describe what students will learn in this course..."
                  className="input-modern w-full h-24 resize-none"
                  required
                />
                <p className="text-xs text-slate-400 mt-1">Write a comprehensive description to attract more students</p>
              </div>

              {/* Difficulty & Career Path */}
              <div className="grid grid-cols-2 gap-6">
                <div>
                  <label className="block text-sm font-semibold mb-2 text-slate-300">Difficulty Level *</label>
                  <select
                    value={difficulty}
                    onChange={(e) => setDifficulty(e.target.value)}
                    className="input-modern w-full"
                  >
                    <option>Beginner</option>
                    <option>Mid-Level</option>
                    <option>Expert</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-semibold mb-2 text-slate-300">Career Path</label>
                  <input
                    type="text"
                    value={career}
                    onChange={(e) => setCareer(e.target.value)}
                    placeholder="e.g., Full Stack Developer"
                    className="input-modern w-full"
                  />
                </div>
              </div>

              {/* Submit Button */}
              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  disabled={loading}
                  className="btn-primary px-8 py-3 font-semibold flex items-center justify-center gap-2 flex-1 disabled:opacity-70"
                >
                  {loading ? (
                    <>
                      <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                      Creating Course...
                    </>
                  ) : (
                    <>
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                      </svg>
                      Create Course
                    </>
                  )}
                </button>

                <button
                  type="reset"
                  className="px-8 py-3 rounded-lg border-2 border-slate-600 hover:border-slate-500 font-semibold text-slate-300 transition-all duration-300"
                >
                  Clear
                </button>
              </div>

              {/* Help Text */}
              <div className="bg-indigo-500/10 border border-indigo-500/30 rounded-lg p-4">
                <h4 className="font-semibold text-indigo-300 mb-2 flex items-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M18 5v8a2 2 0 01-2 2h-5l-5 4v-4H4a2 2 0 01-2-2V5a2 2 0 012-2h12a2 2 0 012 2zm-11-1a1 1 0 11-2 0 1 1 0 012 0z" clipRule="evenodd" />
                  </svg>
                  Tips for Creating a Great Course
                </h4>
                <ul className="text-sm text-slate-300 space-y-1">
                  <li>• Be specific about learning outcomes</li>
                  <li>• Include real-world examples and projects</li>
                  <li>• Use clear, engaging language</li>
                  <li>• Structure content logically with prerequisites</li>
                </ul>
              </div>
            </form>
          </div>
        </div>
      </section>
    </div>
  );
}

import React, { useState, useEffect } from 'react';
import { FiSearch, FiTrendingUp, FiBook } from 'react-icons/fi';
import { useAuth } from '../context/AuthContext';
import { useNavigate } from 'react-router-dom';
import api from '../utils/api';

const Search = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [trendingCourses, setTrendingCourses] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!user) {
      navigate('/login');
      return;
    }
    
    loadTrendingCourses();
  }, [user, navigate]);

  const loadTrendingCourses = async () => {
    try {
      const params = { limit: 50 };
      if (user && user.id) params.user_id = user.id;
      
      const response = await api.get('/courses', { params });
      if (response.data) {
        setTrendingCourses(response.data.slice(0, 6));
      }
    } catch (err) {
      console.error('Error loading trending courses:', err);
    }
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    
    if (!searchQuery.trim()) {
      setError('Please enter a search query');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await api.post('/search/semantic', {
        q: searchQuery,
        user_id: user.id,
      });

      if (response.data) {
        setSearchResults(response.data);
      } else {
        setSearchResults([]);
        setError('No results found');
      }
    } catch (err) {
      console.error('Search error:', err);
      if (err.response?.status === 400) {
        setError(err.response.data.detail || 'Invalid search query');
      } else {
        setError('Error performing search');
      }
      setSearchResults([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-900 text-white pt-20">
      <div className="max-w-6xl mx-auto px-4 py-8">
        {/* Search Header */}
        <div className="mb-12">
          <h1 className="text-4xl font-bold mb-2">Semantic Search</h1>
          <p className="text-slate-400">Find courses using AI-powered semantic search</p>
        </div>

        {/* Search Bar */}
        <form onSubmit={handleSearch} className="mb-12">
          <div className="relative">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search for courses, skills, or topics..."
              className="w-full px-6 py-4 bg-slate-800 border border-slate-700 rounded-lg text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/20"
            />
            <button
              type="submit"
              disabled={loading}
              className="absolute right-3 top-1/2 transform -translate-y-1/2 bg-indigo-600 hover:bg-indigo-700 disabled:bg-slate-600 text-white px-6 py-2 rounded-lg transition-colors flex items-center gap-2"
            >
              <FiSearch className="h-5 w-5" />
              {loading ? 'Searching...' : 'Search'}
            </button>
          </div>
        </form>

        {error && (
          <div className="bg-red-900/20 border border-red-700/50 text-red-200 px-4 py-3 rounded-lg mb-8">
            {error}
          </div>
        )}

        {/* Results Section */}
        {searchResults.length === 0 && !loading && searchQuery && (
          <div className="text-center py-12">
            <FiBook className="h-16 w-16 text-slate-600 mx-auto mb-4" />
            <p className="text-slate-400 text-lg">No courses found for "{searchQuery}"</p>
            <p className="text-slate-500 text-sm mt-2">Try different keywords</p>
          </div>
        )}

        {searchResults.length > 0 && (
          <div>
            <p className="text-slate-300 mb-6">Found {searchResults.length} courses</p>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {searchResults.map((course) => (
                <div
                  key={course.course_id}
                  className="bg-slate-800/50 backdrop-blur-lg rounded-xl overflow-hidden border border-slate-700/50 hover:border-indigo-500/50 transition-all duration-300 group cursor-pointer"
                  onClick={() => navigate(`/courses/${course.course_id}`)}
                >
                  <div className="h-40 bg-gradient-to-br from-indigo-600 to-purple-600 relative overflow-hidden">
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-10 bg-white transition-opacity" />
                    <div className="absolute top-4 right-4 bg-black/50 px-3 py-1 rounded-full text-xs font-medium">
                      {(course.similarity_score * 100).toFixed(0)}% match
                    </div>
                  </div>
                  <div className="p-6">
                    <h3 className="text-lg font-semibold text-white mb-2 line-clamp-2">{course.title}</h3>
                    <p className="text-sm text-slate-400 mb-4 line-clamp-2">{course.description || 'No description'}</p>
                    
                    <div className="flex items-center justify-between mb-4">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-indigo-900/50 text-indigo-200">
                        {course.difficulty || 'Beginner'}
                      </span>
                      <span className="text-xs text-slate-400">{course.career_path || 'General'}</span>
                    </div>

                    <button className="w-full bg-indigo-600 hover:bg-indigo-700 text-white text-sm py-2 rounded-lg transition-colors">
                      View Course
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Trending Section */}
        {trendingCourses.length > 0 && (searchResults.length === 0 || !searchQuery) && (
          <div className="mt-16">
            <h2 className="text-2xl font-bold mb-6 flex items-center gap-2">
              <FiTrendingUp className="h-6 w-6 text-orange-500" />
              Popular Courses
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {trendingCourses.map((course) => (
                <div
                  key={course.id}
                  className="bg-slate-800/50 backdrop-blur-lg rounded-xl overflow-hidden border border-slate-700/50 hover:border-green-500/50 transition-all duration-300 group cursor-pointer"
                  onClick={() => navigate(`/courses/${course.id}`)}
                >
                  <div className="h-40 bg-gradient-to-br from-orange-600 to-red-600 relative overflow-hidden">
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-10 bg-white transition-opacity" />
                  </div>
                  <div className="p-6">
                    <h3 className="text-lg font-semibold text-white mb-2 line-clamp-2">{course.title}</h3>
                    <p className="text-sm text-slate-400 mb-4 line-clamp-2">{course.description || 'No description'}</p>
                    
                    <div className="flex items-center justify-between">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-900/50 text-orange-200">
                        {course.difficulty || 'Beginner'}
                      </span>
                      <span className="text-xs text-slate-400">{course.career_path || 'General'}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default Search;

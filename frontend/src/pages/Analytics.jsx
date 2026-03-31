import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';
import { FiArrowLeft, FiBookOpen, FiAward, FiTrendingUp, FiClock } from 'react-icons/fi';

const Analytics = () => {
  const { user, isAdmin } = useAuth();
  const navigate = useNavigate();
  const [analyticsData, setAnalyticsData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchAnalytics = async () => {
      try {
        if (!isAdmin && user) {
          // Fetch user's learning analytics
          const completedRes = await api.get('/logs/completed-courses', {
            params: { user_id: user.id }
          });
          
          const progressRes = await api.get('/skills/progression-history', {
            params: { user_id: user.id }
          });

          const statusRes = await api.get('/skills/progression-status', {
            params: { user_id: user.id }
          });

          const completedList = completedRes.data.courses || [];
          const progressHistory = progressRes.data.history || [];
          const progressStatus = statusRes.data || {};

          setAnalyticsData({
            totalCoursesCompleted: completedList.length,
            coursesCompleted: completedList,
            progressHistory: progressHistory,
            progressStatus: progressStatus,
            timeSpent: calculateTimeSpent(completedList),
            skillProgression: progressHistory,
          });
        }
        setLoading(false);
      } catch (error) {
        console.error('Error fetching analytics:', error);
        setLoading(false);
      }
    };

    fetchAnalytics();
  }, [user, isAdmin]);

  const calculateTimeSpent = (courses) => {
    // Estimate: 10 hours per beginner, 20 per mid, 30 per expert
    const timeMap = { Beginner: 10, Mid: 20, Expert: 30 };
    return courses.reduce((total, course) => {
      return total + (timeMap[course.difficulty] || 10);
    }, 0);
  };

  const getCoursesByDifficulty = (courses) => {
    return {
      beginner: courses.filter(c => c.difficulty === 'Beginner').length,
      mid: courses.filter(c => c.difficulty === 'Mid').length,
      expert: courses.filter(c => c.difficulty === 'Expert').length,
    };
  };

  const getDifficultyColor = (difficulty) => {
    switch (difficulty) {
      case 'Beginner':
        return 'bg-green-500/20 text-green-300 border border-green-500/30';
      case 'Mid':
        return 'bg-yellow-500/20 text-yellow-300 border border-yellow-500/30';
      case 'Expert':
        return 'bg-red-500/20 text-red-300 border border-red-500/30';
      default:
        return 'bg-slate-500/20 text-slate-300 border border-slate-500/30';
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-900 text-white pt-24">
        <div className="max-w-7xl mx-auto px-6 flex items-center justify-center py-12">
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mb-4"></div>
            <p className="text-slate-400">Loading analytics...</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-900 text-white pt-24">
      <div className="max-w-7xl mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigate('/dashboard')}
              className="p-2 rounded-lg hover:bg-slate-800 transition-colors"
            >
              <FiArrowLeft className="h-5 w-5" />
            </button>
            <div>
              <h1 className="text-3xl font-bold text-white mb-2">Learning Analytics</h1>
              <p className="text-slate-400">Track your learning progress and achievements</p>
            </div>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {/* Courses Completed */}
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 hover:border-slate-600/75 transition-all">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-slate-300 font-medium">Courses Completed</h3>
              <div className="p-3 rounded-lg bg-indigo-500/20">
                <FiBookOpen className="h-5 w-5 text-indigo-400" />
              </div>
            </div>
            <p className="text-4xl font-bold text-white">{analyticsData?.totalCoursesCompleted || 0}</p>
            <p className="text-sm text-slate-400 mt-2">Total courses finished</p>
          </div>

          {/* Current Level */}
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 hover:border-slate-600/75 transition-all">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-slate-300 font-medium">Current Level</h3>
              <div className="p-3 rounded-lg bg-purple-500/20">
                <FiAward className="h-5 w-5 text-purple-400" />
              </div>
            </div>
            <p className="text-4xl font-bold text-white">
              {analyticsData?.progressStatus?.current_level || 'Beginner'}
            </p>
            <p className="text-sm text-slate-400 mt-2">Skill level progression</p>
          </div>

          {/* Estimated Time */}
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 hover:border-slate-600/75 transition-all">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-slate-300 font-medium">Time Invested</h3>
              <div className="p-3 rounded-lg bg-green-500/20">
                <FiClock className="h-5 w-5 text-green-400" />
              </div>
            </div>
            <p className="text-4xl font-bold text-white">{analyticsData?.timeSpent || 0}h</p>
            <p className="text-sm text-slate-400 mt-2">Estimated learning hours</p>
          </div>

          {/* Skill Progress */}
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 hover:border-slate-600/75 transition-all">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-slate-300 font-medium">Progress Rate</h3>
              <div className="p-3 rounded-lg bg-orange-500/20">
                <FiTrendingUp className="h-5 w-5 text-orange-400" />
              </div>
            </div>
            <p className="text-4xl font-bold text-white">
              {analyticsData?.progressHistory && analyticsData.progressHistory.length > 0 
                ? Math.min(100, (analyticsData.progressHistory.length / 3) * 100).toFixed(0)
                : 0}%
            </p>
            <p className="text-sm text-slate-400 mt-2">Progression milestones</p>
          </div>
        </div>

        {/* Courses by Difficulty Breakdown */}
        {analyticsData?.coursesCompleted && analyticsData.coursesCompleted.length > 0 && (
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 mb-8">
            <h2 className="text-xl font-bold text-white mb-6">Courses by Difficulty Level</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {/* Beginner */}
              <div className="bg-slate-900/50 rounded-lg p-4 border border-green-500/30">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-white">Beginner</h3>
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-semibold bg-green-900/50 text-green-200">
                    {getCoursesByDifficulty(analyticsData.coursesCompleted).beginner}
                  </span>
                </div>
                <div className="w-full bg-slate-700 rounded-full h-2">
                  <div 
                    className="bg-green-500 h-2 rounded-full transition-all"
                    style={{
                      width: `${(getCoursesByDifficulty(analyticsData.coursesCompleted).beginner / analyticsData.totalCoursesCompleted) * 100}%`
                    }}
                  ></div>
                </div>
              </div>

              {/* Mid */}
              <div className="bg-slate-900/50 rounded-lg p-4 border border-yellow-500/30">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-white">Mid</h3>
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-semibold bg-yellow-900/50 text-yellow-200">
                    {getCoursesByDifficulty(analyticsData.coursesCompleted).mid}
                  </span>
                </div>
                <div className="w-full bg-slate-700 rounded-full h-2">
                  <div 
                    className="bg-yellow-500 h-2 rounded-full transition-all"
                    style={{
                      width: `${(getCoursesByDifficulty(analyticsData.coursesCompleted).mid / analyticsData.totalCoursesCompleted) * 100}%`
                    }}
                  ></div>
                </div>
              </div>

              {/* Expert */}
              <div className="bg-slate-900/50 rounded-lg p-4 border border-red-500/30">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-white">Expert</h3>
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-semibold bg-red-900/50 text-red-200">
                    {getCoursesByDifficulty(analyticsData.coursesCompleted).expert}
                  </span>
                </div>
                <div className="w-full bg-slate-700 rounded-full h-2">
                  <div 
                    className="bg-red-500 h-2 rounded-full transition-all"
                    style={{
                      width: `${(getCoursesByDifficulty(analyticsData.coursesCompleted).expert / analyticsData.totalCoursesCompleted) * 100}%`
                    }}
                  ></div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Skill Progression Timeline */}
        {analyticsData?.progressHistory && analyticsData.progressHistory.length > 0 && (
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50 mb-8">
            <h2 className="text-xl font-bold text-white mb-6">Skill Progression Timeline</h2>
            <div className="space-y-4">
              {analyticsData.progressHistory.map((progression, index) => (
                <div key={index} className="flex items-start gap-4">
                  <div className="flex flex-col items-center">
                    <div className="w-4 h-4 rounded-full bg-indigo-500 ring-4 ring-indigo-500/20"></div>
                    {index < analyticsData.progressHistory.length - 1 && (
                      <div className="w-1 h-12 bg-slate-700 my-2"></div>
                    )}
                  </div>
                  <div className="flex-1 py-2">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="text-slate-300">{progression.from || progression.previous_level}</span>
                      <span className="text-slate-500">→</span>
                      <span className="font-semibold text-indigo-400">{progression.to || progression.new_level}</span>
                    </div>
                    <p className="text-sm text-slate-400">{progression.reason}</p>
                    <p className="text-xs text-slate-500 mt-1">
                      {new Date(progression.date || progression.created_at).toLocaleDateString()}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Completed Courses */}
        {analyticsData?.coursesCompleted && analyticsData.coursesCompleted.length > 0 && (
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50">
            <h2 className="text-xl font-bold text-white mb-6">Completed Courses</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {analyticsData.coursesCompleted.map((course) => (
                <div key={course.id} className="bg-slate-900/50 rounded-lg p-4 border border-slate-600/30 hover:border-slate-500/50 transition-colors">
                  <div className="flex items-start justify-between mb-2">
                    <h3 className="font-semibold text-white line-clamp-2">{course.title}</h3>
                  </div>
                  <p className="text-xs text-slate-400 mb-3 line-clamp-2">{course.description || 'No description'}</p>
                  <div className="flex items-center justify-between">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getDifficultyColor(course.difficulty)}`}>
                      {course.difficulty || 'Beginner'}
                    </span>
                    <span className="text-xs text-slate-400">{course.career_path || 'General'}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Empty State */}
        {(!analyticsData?.coursesCompleted || analyticsData.coursesCompleted.length === 0) && (
          <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-12 border border-slate-700/50 text-center">
            <FiBookOpen className="h-12 w-12 text-slate-500 mx-auto mb-4" />
            <p className="text-slate-400 text-lg mb-4">No courses completed yet</p>
            <button
              onClick={() => navigate('/dashboard')}
              className="inline-flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-6 py-2 rounded-lg transition-colors"
            >
              Explore Courses
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default Analytics;

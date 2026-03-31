import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import SkillProgress from '../components/SkillProgress';
import api from '../utils/api';
import { 
  FiBook, 
  FiUsers, 
  FiSettings, 
  FiLogOut, 
  FiUser, 
  FiHome, 
  FiBarChart2, 
  FiAward, 
  FiMoreVertical,
  FiCheck,
  FiPlus,
  FiTrash2,
  FiEdit2
} from 'react-icons/fi';

const Dashboard = () => {
  const { user, logout, isAdmin } = useAuth();
  const navigate = useNavigate();
  const [courses, setCourses] = useState([]);
  const [completedCourses, setCompletedCourses] = useState([]);
  const [recommendedCourses, setRecommendedCourses] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('all'); // 'all', 'completed', 'recommended'

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  // Fetch courses on mount
  useEffect(() => {
    const fetchCourses = async () => {
      try {
        const params = {};
        if (user && user.id) params.user_id = user.id;
        
        const response = await api.get('/courses', { params });
        if (response.status === 200) {
          const data = response.data;
          // Handle both array and object with 'value' property
          const courseList = Array.isArray(data) ? data : (data.value || []);
          setCourses(courseList);
        }
      } catch (error) {
        console.error('Error fetching courses:', error);
      }
      
      // Fetch completed courses
      if (user && !isAdmin) {
        try {
          const completedRes = await api.get(`/logs/completed-courses`, {
            params: { user_id: user.id }
          });
          if (completedRes.status === 200) {
            const completedData = completedRes.data;
            const completedList = Array.isArray(completedData) ? completedData : (completedData.courses || []);
            setCompletedCourses(completedList);
          }
        } catch (error) {
          console.error('Error fetching completed courses:', error);
        }
        
        // Fetch recommended courses
        try {
          const recRes = await api.get('/recommendations/home', {
            params: { user_id: user.id, limit: 20 }
          });
          if (recRes.status === 200) {
            const recData = recRes.data;
            const recList = Array.isArray(recData) ? recData : (recData.recommendations || []);
            // Map course_id to id for consistency
            const mappedRecs = recList.map(item => ({
              ...item,
              id: item.course_id || item.id
            }));
            setRecommendedCourses(mappedRecs);
          }
        } catch (error) {
          console.error('Error fetching recommendations:', error);
        }
      }
      
      setLoading(false);
    };

    fetchCourses();
  }, []);


  if (!user) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mb-4"></div>
          <p className="text-slate-400">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-900 text-white">
      {/* Sidebar */}
      <div className="fixed inset-y-0 left-0 w-64 bg-slate-800/80 backdrop-blur-lg border-r border-slate-700/50">
        <div className="p-6">
          <h1 className="text-2xl font-bold bg-gradient-to-r from-indigo-400 to-purple-500 bg-clip-text text-transparent">
            LearnHub
          </h1>
          <p className="text-slate-400 text-sm mt-1">Welcome back, {user.name || user.email}</p>
        </div>

        <nav className="mt-8 px-4 space-y-1">
          <button
            onClick={() => navigate('/dashboard')}
            className="w-full text-left flex items-center px-4 py-3 text-sm font-medium rounded-lg bg-slate-700/50 text-white"
          >
            <FiHome className="mr-3 h-5 w-5 text-indigo-400" />
            Dashboard
          </button>
          
          <button
            onClick={() => navigate('/search')}
            className="w-full text-left flex items-center px-4 py-3 text-sm font-medium rounded-lg text-slate-300 hover:bg-slate-700/30 hover:text-white transition-colors"
          >
            <FiBook className="mr-3 h-5 w-5 text-blue-400" />
            My Courses
          </button>
          
          {isAdmin && (
            <button
              onClick={() => navigate('/admin')}
              className="w-full text-left flex items-center px-4 py-3 text-sm font-medium rounded-lg text-slate-300 hover:bg-slate-700/30 hover:text-white transition-colors"
            >
              <FiUsers className="mr-3 h-5 w-5 text-green-400" />
              Manage Users
            </button>
          )}
          
          <button
            onClick={() => navigate('/analytics')}
            className="w-full text-left flex items-center px-4 py-3 text-sm font-medium rounded-lg text-slate-300 hover:bg-slate-700/30 hover:text-white transition-colors"
          >
            <FiBarChart2 className="mr-3 h-5 w-5 text-purple-400" />
            Analytics
          </button>
          
          <button
            onClick={() => alert('Settings coming soon!')}
            className="w-full text-left flex items-center px-4 py-3 text-sm font-medium rounded-lg text-slate-300 hover:bg-slate-700/30 hover:text-white transition-colors"
          >
            <FiSettings className="mr-3 h-5 w-5 text-yellow-400" />
            Settings
          </button>
        </nav>

        <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-slate-700/50">
          <button
            onClick={handleLogout}
            className="w-full flex items-center justify-between px-4 py-3 text-sm font-medium rounded-lg text-red-400 hover:bg-slate-700/30 hover:text-red-300"
          >
            <span>Sign out</span>
            <FiLogOut className="h-5 w-5" />
          </button>
        </div>
      </div>

      {/* Main content */}
      <div className="pl-64">
        <header className="bg-slate-800/50 backdrop-blur-lg border-b border-slate-700/50">
          <div className="max-w-7xl mx-auto px-6 py-4 flex justify-between items-center">
            <h2 className="text-xl font-semibold text-white">Dashboard</h2>
            <div className="flex items-center space-x-4">
              <div className="relative">
                <button className="p-1 rounded-full text-slate-400 hover:text-white focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-slate-800 focus:ring-indigo-500">
                  <span className="sr-only">View notifications</span>
                  <div className="h-6 w-6 bg-slate-600 rounded-full flex items-center justify-center">
                    <span className="text-xs font-medium">3</span>
                  </div>
                </button>
              </div>
              <div className="relative">
                <div className="flex items-center space-x-2">
                  <div className="h-8 w-8 rounded-full bg-indigo-500 flex items-center justify-center text-white font-medium">
                    {user.name ? user.name.charAt(0).toUpperCase() : user.email.charAt(0).toUpperCase()}
                  </div>
                  <span className="text-sm font-medium text-slate-200">
                    {user.name || user.email.split('@')[0]}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </header>

        <main className="max-w-7xl mx-auto px-6 py-8">
          {/* Skill Progress Widget */}
          {!isAdmin && <SkillProgress />}

          {/* Tabs for course views */}
          {!isAdmin && completedCourses.length > 0 && (
            <div className="mb-8 flex gap-4 mt-8 border-b border-slate-700">
              <button
                onClick={() => setActiveTab('all')}
                className={`pb-4 px-4 font-semibold transition-colors ${
                  activeTab === 'all'
                    ? 'text-indigo-400 border-b-2 border-indigo-400'
                    : 'text-slate-400 hover:text-slate-300'
                }`}
              >
                All Courses
              </button>
              <button
                onClick={() => setActiveTab('completed')}
                className={`pb-4 px-4 font-semibold transition-colors flex items-center gap-2 ${
                  activeTab === 'completed'
                    ? 'text-indigo-400 border-b-2 border-indigo-400'
                    : 'text-slate-400 hover:text-slate-300'
                }`}
              >
                <FiCheck className="h-4 w-4" />
                Completed ({completedCourses.length})
              </button>
              <button
                onClick={() => setActiveTab('recommended')}
                className={`pb-4 px-4 font-semibold transition-colors flex items-center gap-2 ${
                  activeTab === 'recommended'
                    ? 'text-indigo-400 border-b-2 border-indigo-400'
                    : 'text-slate-400 hover:text-slate-300'
                }`}
              >
                <FiAward className="h-4 w-4" />
                Recommended ({recommendedCourses.length})
              </button>
            </div>
          )}

          <div className="mb-8 flex items-center justify-between mt-8">
            <div>
              <h1 className="text-3xl font-bold text-white mb-2">
                {isAdmin ? 'Manage Courses' : 'My Courses'}
              </h1>
              <p className="text-slate-400">
                {isAdmin 
                  ? 'View and manage all courses in the platform' 
                  : 'Explore and enroll in courses'}
              </p>
            </div>
            {isAdmin && (
              <button 
                onClick={() => navigate('/admin/upload')}
                className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg transition-colors"
              >
                <FiPlus className="h-5 w-5" />
                Add Course
              </button>
            )}
          </div>

          {/* Courses Grid */}
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <p className="text-slate-400">Loading courses...</p>
            </div>
          ) : activeTab === 'completed' ? (
            // Completed courses view
            completedCourses.length === 0 ? (
              <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-12 border border-slate-700/50 text-center">
                <FiCheck className="h-12 w-12 text-slate-500 mx-auto mb-4" />
                <p className="text-slate-400 text-lg">No completed courses yet</p>
                <button
                  onClick={() => setActiveTab('all')}
                  className="mt-4 flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg transition-colors mx-auto"
                >
                  <FiBook className="h-5 w-5" />
                  Explore Courses
                </button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {completedCourses.map((course) => (
                  <div key={course.id} className="bg-slate-800/50 backdrop-blur-lg rounded-xl overflow-hidden border border-green-500/50 hover:border-green-400/75 transition-all duration-300 group">
                    <div className="h-40 bg-gradient-to-br from-green-600 to-emerald-600 relative overflow-hidden">
                      <div className="absolute inset-0 opacity-0 group-hover:opacity-10 bg-white transition-opacity" />
                      <div className="absolute top-4 right-4 flex items-center gap-1 bg-green-500/90 px-3 py-1 rounded-full">
                        <FiCheck className="h-4 w-4 text-white" />
                        <span className="text-xs font-semibold text-white">Completed</span>
                      </div>
                    </div>
                    <div className="p-6">
                      <h3 className="text-lg font-semibold text-white mb-2 line-clamp-2">{course.title}</h3>
                      <p className="text-sm text-slate-400 mb-4 line-clamp-2">{course.description || 'No description available'}</p>
                      
                      <div className="flex items-center justify-between mb-4">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-900/50 text-green-200">
                          {course.difficulty || 'Beginner'}
                        </span>
                        <span className="text-xs text-slate-400">{course.career_path || 'General'}</span>
                      </div>

                      <button
                        onClick={() => navigate(`/courses/${course.id}`)}
                        className="w-full bg-green-600 hover:bg-green-700 text-white text-sm py-2 rounded-lg transition-colors"
                      >
                        Review Course
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )
          ) : activeTab === 'recommended' ? (
            // Recommended courses view
            recommendedCourses.length === 0 ? (
              <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-12 border border-slate-700/50 text-center">
                <FiAward className="h-12 w-12 text-slate-500 mx-auto mb-4" />
                <p className="text-slate-400 text-lg">No recommendations yet</p>
                <p className="text-slate-500 text-sm mt-2">Complete some courses to get personalized recommendations</p>
                <button
                  onClick={() => setActiveTab('all')}
                  className="mt-4 flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg transition-colors mx-auto"
                >
                  <FiBook className="h-5 w-5" />
                  Explore Courses
                </button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {recommendedCourses.map((course) => (
                  <div key={course.id} className="bg-slate-800/50 backdrop-blur-lg rounded-xl overflow-hidden border border-amber-500/50 hover:border-amber-400/75 transition-all duration-300 group">
                    <div className="h-40 bg-gradient-to-br from-amber-600 to-orange-600 relative overflow-hidden">
                      <div className="absolute inset-0 opacity-0 group-hover:opacity-10 bg-white transition-opacity" />
                      {course.score && (
                        <div className="absolute top-4 right-4 flex items-center gap-1 bg-amber-500/90 px-3 py-1 rounded-full">
                          <FiAward className="h-4 w-4 text-white" />
                          <span className="text-xs font-semibold text-white">{Math.round(course.score * 10)}%</span>
                        </div>
                      )}
                    </div>
                    <div className="p-6">
                      <h3 className="font-bold text-lg text-white mb-2 line-clamp-2">{course.title}</h3>
                      <p className="text-sm text-slate-400 line-clamp-2 mb-4">{course.description}</p>
                      {course.reason && (
                        <p className="text-xs text-amber-300 mb-4 italic">Why: {course.reason}</p>
                      )}
                      <button
                        onClick={() => navigate(`/courses/${course.id}`)}
                        className="w-full flex items-center justify-center gap-2 bg-amber-600 hover:bg-amber-700 text-white px-4 py-2 rounded-lg transition-colors"
                      >
                        <FiBook className="h-4 w-4" />
                        View Course
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )
          ) : courses.length === 0 ? (
            <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-12 border border-slate-700/50 text-center">
              <FiBook className="h-12 w-12 text-slate-500 mx-auto mb-4" />
              <p className="text-slate-400 text-lg">No courses available yet</p>
              {isAdmin && (
                <button className="mt-4 flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg transition-colors mx-auto">
                  <FiPlus className="h-5 w-5" />
                  Create First Course
                </button>
              )}
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {courses.map((course) => (
                <div key={course.id} className="bg-slate-800/50 backdrop-blur-lg rounded-xl overflow-hidden border border-slate-700/50 hover:border-indigo-500/50 transition-all duration-300 group">
                  <div className="h-40 bg-gradient-to-br from-indigo-600 to-purple-600 relative overflow-hidden">
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-10 bg-white transition-opacity" />
                  </div>
                  <div className="p-6">
                    <h3 className="text-lg font-semibold text-white mb-2 line-clamp-2">{course.title}</h3>
                    <p className="text-sm text-slate-400 mb-4 line-clamp-2">{course.description || 'No description available'}</p>
                    
                    <div className="flex items-center justify-between mb-4">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-indigo-900/50 text-indigo-200">
                        {course.difficulty || 'Beginner'}
                      </span>
                      <span className="text-xs text-slate-400">{course.career_path || 'General'}</span>
                    </div>

                    <div className="flex gap-2">
                      {isAdmin ? (
                        <>
                          <button className="flex-1 flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-700 text-white text-sm py-2 rounded-lg transition-colors">
                            <FiEdit2 className="h-4 w-4" />
                            Edit
                          </button>
                          <button className="flex-1 flex items-center justify-center gap-2 bg-red-600 hover:bg-red-700 text-white text-sm py-2 rounded-lg transition-colors">
                            <FiTrash2 className="h-4 w-4" />
                            Delete
                          </button>
                        </>
                      ) : (
                        <button 
                          onClick={() => navigate(`/courses/${course.id}`)}
                          className="w-full bg-indigo-600 hover:bg-indigo-700 text-white text-sm py-2 rounded-lg transition-colors"
                        >
                          View Course
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </main>
      </div>
    </div>
  );
};

export default Dashboard;

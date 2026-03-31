import React, { useState, useEffect } from 'react';
import { useNavigate, Link, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { FiMail, FiLock, FiEye, FiEyeOff, FiUser, FiShield, FiLogIn } from 'react-icons/fi';

export default function Login() {
  const { login, isAuthenticated, user } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  
  const [formData, setFormData] = useState({
    email: '',
    password: '',
    role: 'user' // Default to user login
  });
  
  const [rememberMe, setRememberMe] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [isLoginAsAdmin, setIsLoginAsAdmin] = useState(false);

  // Redirect if already authenticated
  useEffect(() => {
    if (isAuthenticated) {
      const redirectPath = location.state?.from?.pathname || 
                         (user?.role === 'admin' ? '/admin/dashboard' : '/dashboard');
      navigate(redirectPath, { replace: true });
    }
  }, [isAuthenticated, navigate, location.state, user]);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const toggleLoginType = () => {
    setFormData(prev => ({
      ...prev,
      role: prev.role === 'user' ? 'admin' : 'user'
    }));
    setIsLoginAsAdmin(prev => !prev);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    const { email, password, role } = formData;

    if (!email || !password) {
      setError('Please fill in all fields');
      setLoading(false);
      return;
    }

    try {
      await login(email, password, role);
      
      // The navigation will be handled by the useEffect when isAuthenticated changes
    } catch (err) {
      setError(err.message || 'An error occurred during login. Please try again.');
      console.error('Login error:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full bg-gradient-to-br from-gray-900 to-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md bg-slate-800/50 backdrop-blur-lg rounded-2xl shadow-2xl overflow-hidden border border-slate-700/50">
        <div className="p-8">
          <div className="text-center mb-8">
            <h1 className="text-3xl font-bold text-white mb-2">
              {isLoginAsAdmin ? 'Admin Portal' : 'Welcome Back'}
            </h1>
            <p className="text-slate-400">
              {isLoginAsAdmin ? 'Sign in to access admin dashboard' : 'Sign in to your account'}
            </p>
          </div>

          {/* Role Toggle */}
          <div className="flex items-center justify-center mb-6">
            <div className="inline-flex items-center bg-slate-800/50 rounded-full p-1">
              <button
                type="button"
                onClick={toggleLoginType}
                className={`px-4 py-2 text-sm font-medium rounded-full transition-colors duration-200 ${
                  !isLoginAsAdmin 
                    ? 'bg-indigo-600 text-white shadow' 
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                <div className="flex items-center gap-2">
                  <FiUser className="h-4 w-4" />
                  <span>User Login</span>
                </div>
              </button>
              <button
                type="button"
                onClick={toggleLoginType}
                className={`px-4 py-2 text-sm font-medium rounded-full transition-colors duration-200 ${
                  isLoginAsAdmin 
                    ? 'bg-red-600 text-white shadow' 
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                <div className="flex items-center gap-2">
                  <FiShield className="h-4 w-4" />
                  <span>Admin Login</span>
                </div>
              </button>
            </div>
          </div>

          {error && (
            <div className="mb-6 p-4 bg-red-900/30 text-red-300 border border-red-700/50 rounded-lg text-sm">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-6">
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-slate-300 mb-1">
                Email Address
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <FiMail className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  id="email"
                  name="email"
                  type="email"
                  autoComplete="email"
                  required
                  value={formData.email}
                  onChange={handleChange}
                  className="block w-full pl-10 pr-3 py-3 bg-slate-700/50 border border-slate-600 text-white rounded-lg shadow-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 placeholder-slate-400"
                  placeholder="you@example.com"
                />
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between mb-1">
                <label htmlFor="password" className="block text-sm font-medium text-slate-300">
                  Password
                </label>
                <a href="#" className="text-sm text-indigo-400 hover:text-indigo-300">
                  Forgot password?
                </a>
              </div>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <FiLock className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  id="password"
                  name="password"
                  type={showPassword ? "text" : "password"}
                  autoComplete="current-password"
                  required
                  value={formData.password}
                  onChange={handleChange}
                  className="block w-full pl-10 pr-10 py-3 bg-slate-700/50 border border-slate-600 text-white rounded-lg shadow-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 placeholder-slate-400"
                  placeholder="••••••••"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute inset-y-0 right-0 pr-3 flex items-center"
                >
                  {showPassword ? (
                    <FiEyeOff className="h-5 w-5 text-gray-400 hover:text-gray-300" />
                  ) : (
                    <FiEye className="h-5 w-5 text-gray-400 hover:text-gray-300" />
                  )}
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <input
                  id="remember-me"
                  name="remember-me"
                  type="checkbox"
                  checked={rememberMe}
                  onChange={(e) => setRememberMe(e.target.checked)}
                  className="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-600 rounded bg-slate-700/50"
                />
                <label htmlFor="remember-me" className="ml-2 block text-sm text-slate-300">
                  Remember me
                </label>
              </div>
            </div>

            <div>
              <button
                type="submit"
                disabled={loading}
                className={`w-full flex justify-center items-center gap-2 py-3 px-4 border-2 border-transparent rounded-lg shadow-sm text-sm font-medium text-white ${
                  isLoginAsAdmin 
                    ? 'bg-gradient-to-r from-red-600 to-pink-600 hover:from-red-500 hover:to-pink-500' 
                    : 'bg-gradient-to-r from-indigo-600 to-purple-600 hover:from-indigo-500 hover:to-purple-500'
                } focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-all duration-300 ${
                  loading ? 'opacity-70 cursor-not-allowed' : ''
                }`}
              >
                {loading ? (
                  <>
                    <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.387 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    {isLoginAsAdmin ? 'Signing in as Admin...' : 'Signing in...'}
                  </>
                ) : (
                  <>
                    {isLoginAsAdmin ? <FiShield className="h-5 w-5" /> : <FiLogIn className="h-5 w-5" />}
                    {isLoginAsAdmin ? 'Sign In as Admin' : 'Sign In'}
                  </>
                )}
              </button>
            </div>
          </form>

          {!isLoginAsAdmin && (
            <div className="mt-6 text-center">
              <p className="text-sm text-slate-400">
                Don't have an account?{' '}
                <Link to="/signup" className="font-medium text-indigo-400 hover:text-indigo-300">
                  Sign up
                </Link>
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

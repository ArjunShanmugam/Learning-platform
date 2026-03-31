import React, { createContext, useState, useEffect, useContext, useCallback } from 'react';
import { jwtDecode } from 'jwt-decode';
import { useNavigate, useLocation } from 'react-router-dom';

const AuthContext = createContext(null);

 const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(() => {
    const token = localStorage.getItem('auth_token');
    const userData = localStorage.getItem('user_data');
    
    if (token && userData) {
      try {
        const decoded = jwtDecode(token);
        const user = JSON.parse(userData);
        
        // Verify token hasn't expired
        if (decoded.exp * 1000 < Date.now()) {
          localStorage.removeItem('auth_token');
          localStorage.removeItem('user_data');
          return null;
        }
        
        return user;
      } catch (error) {
        console.error('Error parsing user data:', error);
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user_data');
        return null;
      }
    }
    return null;
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();
  const location = useLocation();

  // Check if user is authenticated
  const isAuthenticated = !!user;

  // Check if user has admin role
  const isAdmin = user?.role === 'admin';

  // Login function
  const login = useCallback(async (email, password, role = 'user') => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000'}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ username: email, password }),
        credentials: 'include',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Login failed');
      }

      // Store token and user data
      localStorage.setItem('auth_token', data.access_token);
      
      // Decode token to get actual role from backend
      const decoded = jwtDecode(data.access_token);
      const actualRole = decoded.role || role;
      
      const userData = {
        id: data.user_id,
        email,
        role: actualRole,
        token: data.access_token
      };
      
      localStorage.setItem('user_data', JSON.stringify(userData));
      setUser(userData);
      
      // Redirect based on actual role from token
      const redirectPath = actualRole === 'admin' ? '/admin/dashboard' : '/dashboard';
      navigate(redirectPath, { replace: true });
      
      return data;
    } catch (err) {
      console.error('Login error:', err);
      setError(err.message || 'An error occurred during login');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [navigate]);

  // Signup function
  const signup = useCallback(async (userData) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000'}/api/auth/signup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ 
          email: userData.email, 
          password: userData.password,
          full_name: userData.full_name || userData.email.split('@')[0]
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Registration failed');
      }

      // Auto-login after successful registration
      await login(userData.email, userData.password, 'user');
      
      return data;
    } catch (err) {
      console.error('Signup error:', err);
      setError(err.message || 'An error occurred during registration');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [login]);

  // Logout function
  const logout = useCallback(() => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('user_data');
    setUser(null);
    navigate('/login');
  }, [navigate]);

  // Check auth status on mount
  useEffect(() => {
    const checkAuth = async () => {
      const token = localStorage.getItem('auth_token');
      
      if (token) {
        try {
          const decoded = jwtDecode(token);
          
          // Check if token is expired
          if (decoded.exp * 1000 < Date.now()) {
            logout();
          } else {
            const userData = JSON.parse(localStorage.getItem('user_data') || '{}');
            setUser(userData);
          }
        } catch (error) {
          console.error('Error checking auth status:', error);
          logout();
        }
      }
      setLoading(false);
    };
    
    checkAuth();
  }, [logout]);

  // Protected route redirect
  useEffect(() => {
    if (!loading && !isAuthenticated && location.pathname !== '/login' && location.pathname !== '/signup') {
      navigate('/login', { state: { from: location }, replace: true });
    }
  }, [loading, isAuthenticated, location, navigate]);

  const value = {
    user,
    isAuthenticated,
    isAdmin,
    loading,
    error,
    login,
    signup,
    logout,
  };

  return <AuthContext.Provider value={value}>{!loading && children}</AuthContext.Provider>;
};

export default AuthContext;
export {AuthContext, AuthProvider, useAuth };

import React from "react";
import { BrowserRouter as Router, Routes, Route, Navigate, Outlet } from "react-router-dom";
import { AuthProvider, AuthContext } from "./context/AuthContext";
import Navbar from "./components/Navbar";
import Home from "./pages/Home";
import Login from "./pages/Login";
import Signup from "./pages/Signup";
import CourseDetail from "./pages/CourseDetail";
import AdminUpload from "./pages/AdminUpload";
import AdminDashboard from "./pages/AdminDashboard";
import SearchPage from "./pages/SearchPage";
import Search from "./pages/Search";
import Dashboard from "./pages/Dashboard";
import Analytics from "./pages/Analytics";
import ProtectedRoute from "./components/ProtectedRoute";

// Redirect to login if not authenticated
const RequireAuth = () => {
  const { user } = React.useContext(AuthContext);
  return user ? <Outlet /> : <Navigate to="/login" replace />;
};

// Redirect to dashboard if already authenticated
const RedirectIfAuthenticated = ({ children }) => {
  const { user } = React.useContext(AuthContext);
  return user ? <Navigate to="/dashboard" replace /> : children;
};

function App() {
  return (
    <Router>
      <AuthProvider>
        <div className="flex flex-col min-h-screen bg-slate-900">
          <Navbar />
          <main className="flex-1 w-full">
            <Routes>
              {/* Public routes */}
              <Route path="/" element={<Home />} />
              <Route 
                path="/login" 
                element={
                  <RedirectIfAuthenticated>
                    <Login />
                  </RedirectIfAuthenticated>
                } 
              />
              <Route 
                path="/signup" 
                element={
                  <RedirectIfAuthenticated>
                    <Signup />
                  </RedirectIfAuthenticated>
                } 
              />
              <Route path="/search" element={<SearchPage />} />
              <Route path="/semantic-search" element={<Search />} />
              <Route path="/courses/:id" element={<CourseDetail />} />

              {/* Protected routes */}
              <Route element={<RequireAuth />}>
                <Route path="/dashboard" element={<Dashboard />} />
                <Route path="/analytics" element={<Analytics />} />
                <Route path="/admin" element={<AdminDashboard />} />
                <Route path="/admin/upload" element={<AdminUpload />} />
              </Route>
              <Route path="*" element={
                <div className="flex items-center justify-center min-h-screen">
                  <div className="text-center">
                    <h1 className="text-4xl font-bold mb-4">404</h1>
                    <p className="text-slate-400 text-lg mb-8">Page not found</p>
                    <a href="/" className="btn-primary px-6 py-2 inline-block">
                      Go Home
                    </a>
                  </div>
                </div>
              } />
            </Routes>
          </main>
          
          {/* Footer */}
          <footer className="border-t border-slate-700 bg-slate-900/50 mt-12 w-full">
            <div className="w-full px-4 py-8">
              <div className="max-w-7xl mx-auto">
                <div className="grid grid-cols-2 md:grid-cols-4 gap-8 mb-8">
                  <div>
                    <h4 className="font-bold mb-4">Product</h4>
                    <ul className="space-y-2 text-slate-400 text-sm">
                      <li><a href="#" className="hover:text-indigo-400 transition">Courses</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Pricing</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Features</a></li>
                    </ul>
                  </div>
                  <div>
                    <h4 className="font-bold mb-4">Company</h4>
                    <ul className="space-y-2 text-slate-400 text-sm">
                      <li><a href="#" className="hover:text-indigo-400 transition">About</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Blog</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Careers</a></li>
                    </ul>
                  </div>
                  <div>
                    <h4 className="font-bold mb-4">Resources</h4>
                    <ul className="space-y-2 text-slate-400 text-sm">
                      <li><a href="#" className="hover:text-indigo-400 transition">Docs</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">API</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Help</a></li>
                    </ul>
                  </div>
                  <div>
                    <h4 className="font-bold mb-4">Legal</h4>
                    <ul className="space-y-2 text-slate-400 text-sm">
                      <li><a href="#" className="hover:text-indigo-400 transition">Privacy</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Terms</a></li>
                      <li><a href="#" className="hover:text-indigo-400 transition">Contact</a></li>
                    </ul>
                  </div>
                </div>
                <div className="border-t border-slate-700 pt-8 flex flex-col md:flex-row items-center justify-between">
                  <p className="text-slate-400 text-sm">&copy; 2024 LearningHub. All rights reserved.</p>
                  <div className="flex gap-4 mt-4 md:mt-0">
                    <a href="#" className="text-slate-400 hover:text-indigo-400 transition">Twitter</a>
                    <a href="#" className="text-slate-400 hover:text-indigo-400 transition">GitHub</a>
                    <a href="#" className="text-slate-400 hover:text-indigo-400 transition">LinkedIn</a>
                  </div>
                </div>
              </div>
            </div>
          </footer>
        </div>
      </AuthProvider>
    </Router>
  );
}

export default App;
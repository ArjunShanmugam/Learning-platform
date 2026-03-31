import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { AuthContext } from "../context/AuthContext";

export default function Navbar() {
  const { user, logout } = React.useContext(AuthContext);
  const navigate = useNavigate();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const handleLogout = () => {
    logout();
    setMobileMenuOpen(false);
    navigate("/login");
  };

  return (
    <nav className="glass sticky top-0 z-50 border-b border-slate-700/50 shadow-lg shadow-black/20">
      <div className="container mx-auto px-4 py-3.5">
        <div className="flex items-center justify-between">
          {/* Logo */}
          <Link 
            to={user ? "/dashboard" : "/"} 
            className="flex items-center gap-3 group cursor-pointer"
          >
            <div className="relative w-10 h-10 bg-gradient-to-br from-indigo-500 via-indigo-400 to-pink-500 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-500/30 group-hover:shadow-indigo-500/50 transition-shadow">
              <span className="text-white font-900 text-lg">LH</span>
            </div>
            <span className="font-800 text-lg text-gradient hidden sm:inline">
              LearningHub
            </span>
          </Link>

          {/* Desktop Menu */}
          <div className="hidden md:flex items-center gap-8">
            {user?.role === 'admin' ? (
              <Link 
                to="/admin/dashboard" 
                className="text-sm font-500 text-slate-300 hover:text-indigo-300 transition-all duration-300 flex items-center gap-2 group px-3 py-2 rounded-lg hover:bg-indigo-500/10"
              >
                <svg className="w-5 h-5 group-hover:scale-110 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                </svg>
                Admin
              </Link>
            ) : (
              <Link 
                to="/dashboard" 
                className="text-sm font-500 text-slate-300 hover:text-indigo-300 transition-all duration-300 flex items-center gap-2 group px-3 py-2 rounded-lg hover:bg-indigo-500/10"
              >
                <svg className="w-5 h-5 group-hover:scale-110 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
                </svg>
                Dashboard
              </Link>
            )}
            <Link 
              to="/search" 
              className="text-sm font-500 text-slate-300 hover:text-indigo-300 transition-all duration-300 flex items-center gap-2 group px-3 py-2 rounded-lg hover:bg-indigo-500/10"
            >
              <svg className="w-5 h-5 group-hover:scale-110 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              Search
            </Link>

            {user ? (
              <div className="flex items-center gap-4 pl-6 border-l border-slate-700/50">
                <div className="text-right">
                  <p className="text-sm font-600 text-slate-100">{user.email.split('@')[0]}</p>
                  <p className="text-xs text-slate-400 capitalize">{user.role || 'user'}</p>
                </div>
                <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-indigo-500 to-pink-500 flex items-center justify-center font-700 text-white shadow-lg shadow-indigo-500/30">
                  {user.email.charAt(0).toUpperCase()}
                </div>
                <button
                  onClick={handleLogout}
                  className="btn-primary text-xs px-4 py-2.5"
                >
                  Logout
                </button>
              </div>
            ) : (
              <Link to="/login" className="btn-primary text-sm px-5 py-2.5">
                Sign In
              </Link>
            )}
          </div>

          {/* Mobile Menu Button */}
          <button 
            className="md:hidden p-2 hover:bg-slate-700/50 rounded-lg transition-colors duration-300"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>
        </div>

        {/* Mobile Menu */}
        {mobileMenuOpen && (
          <div className="md:hidden mt-4 pt-4 border-t border-slate-700/50 space-y-3 animate-fade-in">
            <Link 
              to="/search" 
              className="block px-4 py-3 rounded-lg hover:bg-indigo-500/10 transition-colors text-slate-300 font-500"
              onClick={() => setMobileMenuOpen(false)}
            >
              Search
            </Link>
            {user ? (
              <>
                <div className="px-4 py-3 text-sm text-slate-300 bg-slate-700/40 rounded-lg border border-slate-600/50">
                  Logged in as: {user.email}
                </div>
                <button
                  onClick={handleLogout}
                  className="w-full btn-primary text-sm px-4 py-3"
                >
                  Logout
                </button>
              </>
            ) : (
              <Link 
                to="/login" 
                className="block btn-primary text-sm px-4 py-3 text-center"
                onClick={() => setMobileMenuOpen(false)}
              >
                Sign In
              </Link>
            )}
          </div>
        )}
      </div>
    </nav>
  );
}

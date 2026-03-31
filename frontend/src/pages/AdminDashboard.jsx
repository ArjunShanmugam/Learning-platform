import React from "react";
import { useNavigate } from "react-router-dom";
import { FiPlus, FiUsers, FiBarChart2, FiSettings } from "react-icons/fi";

export default function AdminDashboard() {
  const navigate = useNavigate();

  const adminOptions = [
    {
      title: "Add New Course",
      description: "Create and upload a new course to the platform",
      icon: FiPlus,
      color: "indigo",
      action: () => navigate("/admin/upload")
    },
    {
      title: "Manage Users",
      description: "View and manage user accounts and permissions",
      icon: FiUsers,
      color: "green",
      action: () => alert("User management coming soon!")
    },
    {
      title: "Analytics",
      description: "View platform statistics and performance metrics",
      icon: FiBarChart2,
      color: "purple",
      action: () => navigate("/analytics")
    },
    {
      title: "Settings",
      description: "Configure platform settings and preferences",
      icon: FiSettings,
      color: "yellow",
      action: () => alert("Settings coming soon!")
    }
  ];

  const colorMap = {
    indigo: "bg-indigo-500/20 hover:bg-indigo-500/30 border-indigo-500/50 text-indigo-400",
    green: "bg-green-500/20 hover:bg-green-500/30 border-green-500/50 text-green-400",
    purple: "bg-purple-500/20 hover:bg-purple-500/30 border-purple-500/50 text-purple-400",
    yellow: "bg-yellow-500/20 hover:bg-yellow-500/30 border-yellow-500/50 text-yellow-400"
  };

  return (
    <div className="min-h-screen bg-slate-900">
      {/* Header */}
      <div className="bg-slate-800/50 backdrop-blur-lg border-b border-slate-700/50">
        <div className="max-w-7xl mx-auto px-6 py-8">
          <h1 className="text-4xl font-bold text-white mb-2">Admin Dashboard</h1>
          <p className="text-slate-400">Manage your platform and courses</p>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-6 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {adminOptions.map((option, index) => {
            const Icon = option.icon;
            return (
              <button
                key={index}
                onClick={option.action}
                className={`p-8 rounded-xl border-2 transition-all duration-300 text-left hover:scale-105 ${colorMap[option.color]}`}
              >
                <div className="flex items-start gap-4">
                  <Icon className="h-8 w-8 mt-1 flex-shrink-0" />
                  <div>
                    <h3 className="text-xl font-semibold text-white mb-2">{option.title}</h3>
                    <p className="text-slate-400">{option.description}</p>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

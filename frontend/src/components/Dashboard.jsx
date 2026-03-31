import React from 'react';
import { Link } from 'react-router-dom';

function Dashboard() {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Welcome to Your Learning Dashboard</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Link 
          to="/my-courses" 
          className="p-6 border rounded-lg hover:shadow-md transition-shadow"
        >
          <h2 className="text-xl font-semibold">My Courses</h2>
          <p className="text-gray-600">Continue your learning journey</p>
        </Link>
        <Link 
          to="/browse-courses" 
          className="p-6 border rounded-lg hover:shadow-md transition-shadow"
        >
          <h2 className="text-xl font-semibold">Browse Courses</h2>
          <p className="text-gray-600">Find new courses to take</p>
        </Link>
      </div>
    </div>
  );
}

export default Dashboard;

import React from 'react';
import { Link } from 'react-router-dom';

function AdminDashboard() {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Admin Dashboard</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Link 
          to="/admin/courses" 
          className="p-6 border rounded-lg hover:shadow-md transition-shadow"
        >
          <h2 className="text-xl font-semibold">Manage Courses</h2>
          <p className="text-gray-600">Create, update, or delete courses</p>
        </Link>
        <Link 
          to="/admin/users" 
          className="p-6 border rounded-lg hover:shadow-md transition-shadow"
        >
          <h2 className="text-xl font-semibold">Manage Users</h2>
          <p className="text-gray-600">View and manage user accounts</p>
        </Link>
      </div>
    </div>
  );
}

export default AdminDashboard;

import { Navigate, Outlet } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

const ProtectedRoute = ({ requiredRole = 'user' }) => {
  const { user } = useAuth();
  
  if (!user) {
    return <Navigate to="/login" />;
  }

  if (requiredRole === 'admin' && user.role !== 'admin') {
    return <Navigate to="/" />;
  }

  return <Outlet />;
};

export default ProtectedRoute;

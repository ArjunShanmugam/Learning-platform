import React, { useState, useEffect } from 'react';
import { FiTrendingUp, FiAward, FiTarget } from 'react-icons/fi';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';

const SkillProgress = () => {
  const { user } = useAuth();
  const [progressStatus, setProgressStatus] = useState(null);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (user) {
      loadProgressStatus();
      loadProgressHistory();
    }
  }, [user]);

  const loadProgressStatus = async () => {
    try {
      const response = await api.get('/skills/progression-status');
      if (response.status === 200) {
        setProgressStatus(response.data);
      } else {
        // Set default progression status if endpoint fails
        setProgressStatus({
          current_level: 'Beginner',
          next_level: 'Mid',
          can_progress: false,
          progress: 0,
          required: 2,
          required_difficulty: 'Mid'
        });
      }
    } catch (err) {
      console.error('Error loading progression status:', err);
      // Set default progression status on error
      setProgressStatus({
        current_level: 'Beginner',
        next_level: 'Mid',
        can_progress: false,
        progress: 0,
        required: 2,
        required_difficulty: 'Mid'
      });
    } finally {
      setLoading(false);
    }
  };

  const loadProgressHistory = async () => {
    try {
      const response = await api.get('/skills/progression-history');
      if (response.status === 200) {
        setHistory(response.data.history || []);
      }
    } catch (err) {
      console.error('Error loading progression history:', err);
      setHistory([]);
    }
  };

  const applyProgression = async () => {
    try {
      const res = await api.post('/skills/check-progression');
      if (res.status === 200) {
        if (res.data && res.data.status === 'progressed') {
          // Refresh status and history
          await loadProgressStatus();
          await loadProgressHistory();
          alert(res.data.message || `Promoted to ${res.data.new_level}`);
        } else if (res.data && res.data.status === 'no_progression') {
          const prog = res.data.progress || {};
          // If career requirement is not met, show a more specific message
          if (prog.career_requirement_met === false && prog.career_path) {
            alert(`You're close! At least one of the required courses must be in your career path (${prog.career_path}).`);
          } else {
            alert(res.data.message || 'Not yet eligible to progress');
          }
          setProgressStatus(prog);
        } else {
          alert(res.data.message || 'No progression applied');
        }
      }
    } catch (err) {
      console.error('Error applying progression:', err);
      alert('Failed to apply progression');
    }
  };

  if (loading) {
    return (
      <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-3 rounded-lg bg-purple-500/20">
            <FiTrendingUp className="h-6 w-6 text-purple-400" />
          </div>
          <div className="animate-pulse">
            <div className="h-4 w-32 bg-slate-700 rounded mb-2"></div>
            <div className="h-3 w-48 bg-slate-700 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (!progressStatus) {
    return null;
  }

  const skillLevels = ['Beginner', 'Mid', 'Expert'];
  const currentIndex = skillLevels.indexOf(progressStatus.current_level);
  const progressPercentage = progressStatus.can_progress 
    ? 100 
    : (progressStatus.progress / progressStatus.required) * 100;

  return (
    <div className="bg-slate-800/50 backdrop-blur-lg rounded-xl p-6 border border-slate-700/50">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="p-3 rounded-lg bg-purple-500/20">
            <FiTrendingUp className="h-6 w-6 text-purple-400" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-white">Skill Progression</h3>
            <p className="text-sm text-slate-400">Track your learning journey</p>
          </div>
        </div>
        <div className="text-right">
          <div className="text-2xl font-bold text-purple-400">{progressStatus.current_level}</div>
          <div className="text-xs text-slate-400">Current Level</div>
        </div>
      </div>

      {/* Skill Level Progress */}
      <div className="mb-6">
        <div className="flex justify-between items-center mb-2">
          <span className="text-sm font-medium text-slate-300">Level Progress</span>
          {progressStatus.next_level && (
            <span className="text-xs text-slate-400">
              {progressStatus.progress} / {progressStatus.required} {progressStatus.required_difficulty} courses
              {progressStatus.career_path && progressStatus.completed_with_career !== undefined && (
                <span className="ml-2 text-xs text-slate-500">( {progressStatus.completed_with_career} in {progressStatus.career_path} )</span>
              )}
            </span>
          )}
        </div>
        
        {progressStatus.next_level ? (
          <>
            <div className="w-full bg-slate-700/50 rounded-full h-3 overflow-hidden">
              <div
                className="bg-gradient-to-r from-purple-500 to-pink-500 h-full transition-all duration-500"
                style={{ width: `${Math.min(progressPercentage, 100)}%` }}
              />
            </div>
            <div className="mt-2 text-xs text-slate-400 flex items-center justify-between">
              <div>
                {progressStatus.can_progress 
                  ? '🎉 Ready to level up!' 
                  : `${progressStatus.required - progressStatus.progress} more courses to reach ${progressStatus.next_level}`}
              </div>
              {progressStatus.can_progress && (
                <div>
                  <button
                    onClick={applyProgression}
                    className="ml-4 inline-flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded-lg text-xs"
                  >
                    Level up to {progressStatus.next_level}
                  </button>
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="text-sm text-slate-400">You've reached the maximum skill level!</div>
        )}
      </div>

      {/* Skill Level Timeline */}
      <div className="mb-6">
        <h4 className="text-sm font-medium text-slate-300 mb-4">Skill Levels</h4>
        <div className="flex gap-2">
          {skillLevels.map((level, idx) => (
            <div key={level} className="flex-1">
              <div
                className={`h-2 rounded-full transition-all ${
                  idx <= currentIndex
                    ? 'bg-gradient-to-r from-purple-500 to-pink-500'
                    : 'bg-slate-700/50'
                }`}
              />
              <div className="text-xs text-slate-400 mt-1 text-center">{level}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Progression History */}
      {history.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-slate-300 mb-3">Recent Achievements</h4>
          <div className="space-y-2">
            {history.slice(0, 3).map((item, idx) => (
              <div key={idx} className="flex items-start gap-3 p-3 bg-slate-700/30 rounded-lg">
                <FiAward className="h-4 w-4 text-yellow-400 mt-1 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-white">
                    Promoted to <span className="font-semibold text-purple-400">{item.to}</span>
                  </p>
                  <p className="text-xs text-slate-400">{item.reason}</p>
                  {item.date && (
                    <p className="text-xs text-slate-500 mt-1">
                      {new Date(item.date).toLocaleDateString()}
                    </p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Call to Action */}
      {progressStatus.next_level && !progressStatus.can_progress && (
        <div className="mt-6 p-4 bg-purple-900/20 border border-purple-700/50 rounded-lg">
          <div className="flex items-start gap-3">
            <FiTarget className="h-5 w-5 text-purple-400 mt-0.5 flex-shrink-0" />
            <div>
              <p className="text-sm font-medium text-purple-200">Keep Learning!</p>
              <p className="text-xs text-purple-300 mt-1">
                Complete {progressStatus.required - progressStatus.progress} more {progressStatus.required_difficulty} level courses to reach {progressStatus.next_level}
                {progressStatus.career_requirement_met === false && progressStatus.career_path && (
                  <span className="block mt-2 text-xs text-purple-300">At least one of the required courses must be in your career path ({progressStatus.career_path}). Try searching for courses that match your career goal.</span>
                )}
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default SkillProgress;

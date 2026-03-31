#!/usr/bin/env python3
"""
Test script to verify if recommendations are working based on:
1. Completed courses
2. User interactions (clicks, views)
3. Career path and skill level
4. Collaborative filtering (similar users)
"""

import sys
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models import (
    User, UserProfile, Course, CompletedCourse, 
    UserInteraction, CourseEmbedding
)
from app.services.recommender_service import get_recommender_service
from sqlalchemy import func

def analyze_user_data(user_id: int, db: Session):
    """Analyze user's learning journey"""
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        print(f"❌ User {user_id} not found")
        return False
    
    print("\n" + "="*80)
    print(f"📊 USER DATA ANALYSIS - User ID: {user_id}")
    print("="*80)
    
    print(f"\n👤 User Profile:")
    print(f"   Email: {user.email}")
    print(f"   Created: {user.created_at}")
    
    if user.profile:
        print(f"   Skill Level: {user.profile.skill_level or 'Not set'}")
        print(f"   Career Path: {user.profile.career_path or 'Not set'}")
        print(f"   Role: {user.profile.role or 'Student'}")
    else:
        print(f"   ⚠️  No profile data!")
    
    # Completed courses
    print(f"\n✅ COMPLETED COURSES:")
    completed = db.query(CompletedCourse).filter(
        CompletedCourse.user_id == user_id
    ).all()
    
    if not completed:
        print(f"   ⚠️  No completed courses")
    else:
        print(f"   Total: {len(completed)}")
        for cc in completed:
            course = db.query(Course).filter(Course.id == cc.course_id).first()
            if course:
                print(f"   - {course.title}")
                print(f"     Difficulty: {course.difficulty}, Career: {course.career_path}")
    
    # User interactions
    print(f"\n🖱️  USER INTERACTIONS (Clicks, Views, etc):")
    interactions = db.query(UserInteraction).filter(
        UserInteraction.user_id == user_id
    ).all()
    
    if not interactions:
        print(f"   ⚠️  No interaction data logged")
        print(f"   💡 Make sure to click courses, view them, and start learning")
    else:
        print(f"   Total Interactions: {len(interactions)}")
        
        # Group by type
        interaction_types = {}
        for interaction in interactions:
            itype = interaction.interaction_type or "unknown"
            if itype not in interaction_types:
                interaction_types[itype] = []
            interaction_types[itype].append(interaction.course_id)
        
        for itype, course_ids in interaction_types.items():
            print(f"\n   {itype.upper()} ({len(course_ids)} interactions):")
            for course_id in set(course_ids)[:5]:  # Show first 5 unique courses
                course = db.query(Course).filter(Course.id == course_id).first()
                if course:
                    count = course_ids.count(course_id)
                    print(f"      - {course.title} ({count}x)")
    
    # Collaborative filtering data
    print(f"\n👥 COLLABORATIVE FILTERING DATA:")
    if user.profile and user.profile.career_path:
        cohort_size = db.query(func.count(UserProfile.id)).filter(
            UserProfile.career_path == user.profile.career_path,
            UserProfile.user_id != user_id
        ).scalar()
        print(f"   Career Path Cohort Size: {cohort_size} other users")
        
        # Get popular courses in cohort
        popular_in_cohort = db.query(
            Course.id,
            Course.title,
            func.count(UserInteraction.id).label('interaction_count')
        ).join(
            UserInteraction, Course.id == UserInteraction.course_id
        ).join(
            User, UserInteraction.user_id == User.id
        ).join(
            UserProfile, User.id == UserProfile.user_id
        ).filter(
            UserProfile.career_path == user.profile.career_path
        ).group_by(Course.id).order_by(
            func.count(UserInteraction.id).desc()
        ).limit(5).all()
        
        if popular_in_cohort:
            print(f"   Popular courses in your cohort:")
            for course_id, title, count in popular_in_cohort:
                print(f"      - {title} ({count} interactions)")
    else:
        print(f"   ⚠️  No career path set - collaborative filtering limited")
    
    return True

def test_recommendations(user_id: int, db: Session):
    """Test the recommendation system"""
    print(f"\n" + "="*80)
    print(f"🎯 RECOMMENDATION TEST - Generating recommendations...")
    print("="*80)
    
    recommender = get_recommender_service()
    
    try:
        recommendations = recommender.get_hybrid_recommendations(
            user_id=user_id,
            db=db,
            k=20
        )
        
        if not recommendations:
            print("\n❌ NO RECOMMENDATIONS GENERATED")
            print("   Possible reasons:")
            print("   1. User has completed all courses")
            print("   2. No user profile found")
            print("   3. Recommendation service has issues")
            return False
        
        print(f"\n✅ Generated {len(recommendations)} recommendations\n")
        
        print("📋 TOP 10 RECOMMENDATIONS:")
        print("-"*80)
        
        for idx, rec in enumerate(recommendations[:10], 1):
            course = db.query(Course).filter(Course.id == rec['course_id']).first()
            
            print(f"\n{idx}. {rec['title']}")
            print(f"   Score: {rec['score']:.3f}")
            if 'content_score' in rec:
                print(f"   Content Score: {rec['content_score']:.3f}")
            if 'collab_score' in rec:
                print(f"   Collab Score: {rec['collab_score']:.3f}")
            print(f"   Difficulty: {rec['difficulty']}")
            print(f"   Career: {rec['career_path']}")
            print(f"   Reason: {rec['reason']}")
        
        # Analysis
        print(f"\n" + "="*80)
        print("📊 RECOMMENDATION ANALYSIS")
        print("="*80)
        
        scores = [r['score'] for r in recommendations[:10]]
        print(f"\nScore Statistics (Top 10):")
        print(f"   Min Score: {min(scores):.3f}")
        print(f"   Max Score: {max(scores):.3f}")
        print(f"   Avg Score: {sum(scores)/len(scores):.3f}")
        
        user = db.query(User).filter(User.id == user_id).first()
        if user and user.profile:
            career_matches = sum(1 for r in recommendations[:10]
                                if r.get('career_path', '').lower() == 
                                   (user.profile.career_path or '').lower())
            print(f"\nCareer Path Matching: {career_matches}/10 courses match your career")
        
        difficulty_dist = {}
        for rec in recommendations[:10]:
            diff = rec.get('difficulty', 'Unknown')
            difficulty_dist[diff] = difficulty_dist.get(diff, 0) + 1
        
        print(f"\nDifficulty Distribution:")
        for diff, count in sorted(difficulty_dist.items()):
            print(f"   {diff}: {count}")
        
        print(f"\n✅ Recommendation system appears to be working!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error generating recommendations: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_my_recommendations.py <user_id>")
        print("Example: python test_my_recommendations.py 1")
        sys.exit(1)
    
    try:
        user_id = int(sys.argv[1])
    except ValueError:
        print(f"❌ Invalid user ID: {sys.argv[1]}")
        sys.exit(1)
    
    db = SessionLocal()
    
    try:
        # Step 1: Analyze user data
        if not analyze_user_data(user_id, db):
            return
        
        # Step 2: Test recommendations
        test_recommendations(user_id, db)
        
        print(f"\n" + "="*80)
        print("✅ TEST COMPLETE")
        print("="*80)
        print("\n💡 TROUBLESHOOTING TIPS:")
        print("   1. Completed Courses: If none, complete a course first")
        print("   2. Interactions: If none, click on courses to log interactions")
        print("   3. Career Path: Set your career path in user profile for better recommendations")
        print("   4. Skill Level: Update your skill level for personalized difficulty matching")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()

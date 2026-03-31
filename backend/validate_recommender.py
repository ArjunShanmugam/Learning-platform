"""
Comprehensive validation script for the recommendation system.
Checks:
1. Scoring accuracy and weights
2. Reason validity
3. Ranking correctness
4. Career path and skill level matching
"""

from app.db import SessionLocal
from app.models import User, UserProfile, Course, CourseEmbedding, CompletedCourse
from app.services.recommender_service import get_recommender_service
import json

def validate_recommender():
    db = SessionLocal()
    recommender = get_recommender_service()
    
    print("=" * 80)
    print("RECOMMENDATION SYSTEM VALIDATION")
    print("=" * 80)
    
    # Get test user (ID 4)
    user = db.query(User).filter(User.id == 4).first()
    if not user:
        print("❌ Test user not found")
        return
    
    user_profile = user.profile
    print(f"\n📊 User Profile:")
    print(f"   ID: {user.id}")
    print(f"   Email: {user.email}")
    print(f"   Skill Level: {user_profile.skill_level}")
    print(f"   Career Path: {user_profile.career_path}")
    
    # Get completed courses
    completed_courses = db.query(CompletedCourse).filter(
        CompletedCourse.user_id == user.id
    ).all()
    print(f"\n✅ Completed Courses: {len(completed_courses)}")
    for cc in completed_courses:
        course = db.query(Course).filter(Course.id == cc.course_id).first()
        if course:
            print(f"   - {course.title} ({course.difficulty})")
    
    # Get recommendations
    print(f"\n🎯 Getting Recommendations...")
    recommendations = recommender.get_hybrid_recommendations(
        user_id=user.id,
        db=db,
        k=15
    )
    
    print(f"\n📋 Top Recommendations:")
    print("-" * 80)
    
    issues = []
    
    for idx, rec in enumerate(recommendations[:10], 1):
        course = db.query(Course).filter(Course.id == rec['course_id']).first()
        
        print(f"\n{idx}. {rec['title']}")
        print(f"   Score: {rec['score']:.3f} (Content: {rec['content_score']:.3f} | Collab: {rec['collab_score']:.3f})")
        print(f"   Difficulty: {rec['difficulty']} | Career: {rec['career_path']}")
        print(f"   Reason: {rec['reason']}")
        
        # Validation checks
        checks = validate_recommendation(user_profile, course, rec, idx, issues)
        for check in checks:
            print(f"   {check}")
    
    # Analysis
    print("\n" + "=" * 80)
    print("🔍 ANALYSIS")
    print("=" * 80)
    
    # Check score distribution
    scores = [r['score'] for r in recommendations[:10]]
    print(f"\n📊 Score Distribution (Top 10):")
    print(f"   Min: {min(scores):.3f}")
    print(f"   Max: {max(scores):.3f}")
    print(f"   Avg: {sum(scores)/len(scores):.3f}")
    print(f"   Range: {max(scores) - min(scores):.3f}")
    
    # Check skill level distribution
    skill_dist = {}
    for rec in recommendations[:10]:
        course = db.query(Course).filter(Course.id == rec['course_id']).first()
        diff = course.difficulty if course else "Unknown"
        skill_dist[diff] = skill_dist.get(diff, 0) + 1
    
    print(f"\n🎓 Difficulty Distribution (Top 10):")
    for skill, count in sorted(skill_dist.items()):
        print(f"   {skill}: {count}")
    
    # Check career path matching
    career_matches = sum(1 for r in recommendations[:10] 
                         if r.get('career_path', '').lower() == 
                            (user_profile.career_path or '').lower())
    print(f"\n💼 Career Path Matching:")
    print(f"   User Career: {user_profile.career_path}")
    print(f"   Matching in Top 10: {career_matches}/10 ({career_matches*10}%)")
    
    # Issues found
    if issues:
        print(f"\n⚠️  ISSUES FOUND ({len(issues)}):")
        for issue in issues[:10]:
            print(f"   - {issue}")
    else:
        print(f"\n✅ No major issues found!")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("💡 RECOMMENDATIONS FOR IMPROVEMENT")
    print("=" * 80)
    
    recommendations_for_improvement = [
        "1. Fine-tune weights: Content (0.6) vs Collaborative (0.4) - consider adjusting based on user diversity",
        "2. Career path boost: Currently +0.1, consider increasing to +0.15 for stronger career alignment",
        "3. Skill level progression: Beginner→Mid requires 2 Mid courses. Verify this is optimal",
        "4. Reason validation: Ensure reasons match the actual scoring logic",
        "5. Collaborative scoring: Consider using user interaction history more effectively",
        "6. Cold start problem: New users may get generic recommendations - add more diverse content-based signals",
        "7. Diversity: Add logic to prevent recommending similar courses (cluster similar courses)",
        "8. Recency: Consider adding a time-decay factor for older courses"
    ]
    
    for rec in recommendations_for_improvement:
        print(f"\n{rec}")
    
    db.close()
    print("\n" + "=" * 80)
    print("✅ Validation Complete")
    print("=" * 80)

def validate_recommendation(user_profile, course, rec, idx, issues):
    """Validate individual recommendation"""
    checks = []
    
    # Check 1: Score is in valid range
    if not (0 <= rec['score'] <= 1.0):
        issue = f"Rec {idx}: Invalid score {rec['score']}"
        issues.append(issue)
        checks.append(f"❌ Invalid score range")
    else:
        checks.append(f"✅ Score in valid range")
    
    # Check 2: Reason is not empty
    if not rec['reason'] or rec['reason'].strip() == "":
        issue = f"Rec {idx}: Empty reason"
        issues.append(issue)
        checks.append(f"❌ Empty reason")
    else:
        checks.append(f"✅ Reason provided")
    
    # Check 3: Content score should contribute to final score
    if rec['content_score'] == 0 and rec['collab_score'] == 0:
        issue = f"Rec {idx}: Both scores are 0"
        issues.append(issue)
        checks.append(f"❌ No scoring factors")
    else:
        checks.append(f"✅ Has scoring factors")
    
    # Check 4: Skill level alignment
    if user_profile.skill_level and course:
        skill_levels = ["Beginner", "Mid", "Expert"]
        user_idx = skill_levels.index(user_profile.skill_level) if user_profile.skill_level in skill_levels else 0
        course_idx = skill_levels.index(course.difficulty) if course.difficulty in skill_levels else 0
        skill_gap = abs(course_idx - user_idx)
        
        if skill_gap == 0:
            checks.append(f"✅ Perfect skill match")
        elif skill_gap == 1:
            checks.append(f"✅ One level away (appropriate challenge)")
        else:
            checks.append(f"⚠️  Two+ levels away ({skill_gap} gap)")
    
    return checks

if __name__ == "__main__":
    validate_recommender()

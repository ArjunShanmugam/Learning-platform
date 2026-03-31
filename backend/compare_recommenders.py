"""
Comparison test: Original vs Improved Recommender
"""

from app.db import SessionLocal
from app.models import User, Course
from app.services.recommender_service import get_recommender_service
from app.services.recommender_service_v2 import get_recommender_service_v2

def compare_recommenders():
    db = SessionLocal()
    
    print("=" * 100)
    print("RECOMMENDER COMPARISON: Original vs Improved")
    print("=" * 100)
    
    # Test user
    user = db.query(User).filter(User.id == 4).first()
    if not user:
        print("❌ User not found")
        return
    
    print(f"\n👤 User: {user.email} | Skill: {user.profile.skill_level} | Career: {user.profile.career_path}")
    
    # Get recommendations from both services
    original_svc = get_recommender_service()
    improved_svc = get_recommender_service_v2()
    
    original_recs = original_svc.get_hybrid_recommendations(user.id, db, k=10)
    improved_recs = improved_svc.get_hybrid_recommendations(user.id, db, k=10)
    
    # Compare
    print("\n" + "=" * 100)
    print("TOP 10 RECOMMENDATIONS COMPARISON")
    print("=" * 100)
    
    print(f"\n{'Rank':<6} {'ORIGINAL':<40} {'Score':<10} {'IMPROVED':<40} {'Score':<10}")
    print("-" * 100)
    
    for i in range(max(len(original_recs), len(improved_recs))):
        orig_title = original_recs[i]['title'][:38] if i < len(original_recs) else "N/A"
        orig_score = f"{original_recs[i]['score']:.3f}" if i < len(original_recs) else "N/A"
        
        improved_title = improved_recs[i]['title'][:38] if i < len(improved_recs) else "N/A"
        improved_score = f"{improved_recs[i]['score']:.3f}" if i < len(improved_recs) else "N/A"
        
        print(f"{i+1:<6} {orig_title:<40} {orig_score:<10} {improved_title:<40} {improved_score:<10}")
    
    # Detailed comparison of top 5
    print("\n" + "=" * 100)
    print("DETAILED ANALYSIS - TOP 5")
    print("=" * 100)
    
    for i in range(min(5, len(original_recs), len(improved_recs))):
        orig = original_recs[i]
        impr = improved_recs[i]
        
        print(f"\n--- Position {i+1} ---")
        print(f"Original: {orig['title']}")
        print(f"  Score: {orig['score']:.3f} (Content: {orig['content_score']:.3f} | Collab: {orig['collab_score']:.3f})")
        print(f"  Reason: {orig['reason']}")
        
        print(f"Improved: {impr['title']}")
        print(f"  Score: {impr['score']:.3f} (Content: {impr['content_score']:.3f} | Collab: {impr['collab_score']:.3f})")
        print(f"  Reason: {impr['reason']}")
        
        # Show difference
        score_diff = impr['score'] - orig['score']
        if score_diff > 0.05:
            print(f"  → Improved by {score_diff:.3f} ✅")
        elif score_diff < -0.05:
            print(f"  → Decreased by {abs(score_diff):.3f} ⚠️")
        else:
            print(f"  → Similar ({score_diff:+.3f})")
    
    # Statistics
    print("\n" + "=" * 100)
    print("STATISTICS")
    print("=" * 100)
    
    orig_scores = [r['score'] for r in original_recs[:10]]
    impr_scores = [r['score'] for r in improved_recs[:10]]
    
    print(f"\nOriginal Recommender:")
    print(f"  Avg Score: {sum(orig_scores)/len(orig_scores):.3f}")
    print(f"  Max Score: {max(orig_scores):.3f}")
    print(f"  Min Score: {min(orig_scores):.3f}")
    print(f"  Range: {max(orig_scores) - min(orig_scores):.3f}")
    print(f"  Ties (0.300): {sum(1 for s in orig_scores if s == 0.300)}")
    
    print(f"\nImproved Recommender:")
    print(f"  Avg Score: {sum(impr_scores)/len(impr_scores):.3f}")
    print(f"  Max Score: {max(impr_scores):.3f}")
    print(f"  Min Score: {min(impr_scores):.3f}")
    print(f"  Range: {max(impr_scores) - min(impr_scores):.3f}")
    print(f"  Ties: {len(set(impr_scores)) == len(impr_scores)}")  # Check for ties
    
    # Career matching
    orig_career_match = sum(1 for r in original_recs[:10] 
                           if r.get('career_path', '').lower() == 
                              (user.profile.career_path or '').lower())
    impr_career_match = sum(1 for r in improved_recs[:10] 
                           if r.get('career_path', '').lower() == 
                              (user.profile.career_path or '').lower())
    
    print(f"\nCareer Path Matching ({user.profile.career_path}):")
    print(f"  Original: {orig_career_match}/10 ({orig_career_match*10}%)")
    print(f"  Improved: {impr_career_match}/10 ({impr_career_match*10}%)")
    
    # Diversity
    orig_combos = len(set((r['difficulty'], r.get('career_path', 'general')) for r in original_recs[:10]))
    impr_combos = len(set((r['difficulty'], r.get('career_path', 'general')) for r in improved_recs[:10]))
    
    print(f"\nDiversity (unique skill/career combinations):")
    print(f"  Original: {orig_combos}/10")
    print(f"  Improved: {impr_combos}/10")
    
    print("\n" + "=" * 100)
    print("✅ Comparison Complete")
    print("=" * 100)
    
    db.close()

if __name__ == "__main__":
    compare_recommenders()

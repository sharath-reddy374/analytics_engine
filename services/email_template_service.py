from typing import Dict, Any, Optional
import logging
from datetime import datetime
import asyncio
from services.llm_service import LLMService

logger = logging.getLogger(__name__)

class EmailTemplateService:
    """
    Service for generating dynamic educational email content using OpenAI
    Eliminates the need for predefined templates by generating contextual content
    """
    
    def __init__(self):
        self.llm_service = LLMService()
    
    def generate_email_content(self, template_id: str, features: Dict[str, Any], ai_triggers: list = None) -> Optional[Dict[str, str]]:
        """
        Generate educational email content dynamically using OpenAI
        
        Args:
            template_id: Rule identifier (used for context)
            features: User features dictionary
            ai_triggers: AI-generated triggers and insights
            
        Returns:
            Dictionary with 'subject' and 'content' keys
        """
        try:
            # Prepare comprehensive data for OpenAI generation
            email_context = {
                'user_profile': {
                    'learning_level': self._determine_learning_level(features),
                    'primary_subjects': self._extract_subjects_from_topics(features.get('top_topics', [])),
                    'engagement_level': self._assess_engagement_level(features),
                    'recent_activity': {
                        'conversations': features.get('conversations_7d', 0),
                        'tests_taken': features.get('tests_7d', 0),
                        'minutes_studied': features.get('minutes_7d', 0),
                        'courses_active': features.get('active_courses', 0)
                    }
                },
                'learning_insights': {
                    'top_topics': features.get('top_topics', []),
                    'strong_subjects': features.get('strong_subjects', []),
                    'weak_subjects': features.get('weak_subjects', []),
                    'test_accuracy': features.get('test_accuracy', 0),
                    'course_completion': features.get('icp_completion_rate', 0)
                },
                'ai_triggers': ai_triggers or [],
                'email_purpose': self._determine_email_purpose(template_id, features, ai_triggers),
                'personalization': {
                    'recency_days': features.get('recency_days', 0),
                    'frequency': features.get('frequency_7d', 0),
                    'improvement_trend': features.get('itp_improvement_trend', 0)
                }
            }
            
            # Generate content using OpenAI
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                email_result = loop.run_until_complete(
                    self.llm_service.generate_educational_email(email_context)
                )
                loop.close()
                
                return {
                    'subject': email_result.get('subject', 'Your Learning Journey Continues!'),
                    'content': email_result.get('content', self._generate_fallback_content(email_context)),
                    'template_id': template_id,
                    'generated_at': datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.warning(f"OpenAI generation failed, using educational fallback: {str(e)}")
                return {
                    'subject': self._generate_educational_subject(email_context),
                    'content': self._generate_educational_fallback(email_context),
                    'template_id': template_id,
                    'generated_at': datetime.now().isoformat()
                }
            
        except Exception as e:
            logger.error(f"Failed to generate email content for {template_id}: {str(e)}")
            return None
    
    def _determine_learning_level(self, features: Dict[str, Any]) -> str:
        """Determine educational level from user activity and topics"""
        topics = features.get('top_topics', [])
        
        # Check for advanced topics
        advanced_indicators = ['USMLE', 'Medicine', 'Pharmacology', 'Immunology', 'Pathology']
        if any(indicator in ' '.join(topics) for indicator in advanced_indicators):
            return 'graduate_medical'
        
        # Check for high school/college topics
        intermediate_indicators = ['Biology', 'Chemistry', 'Physics', 'History', 'Algebra']
        if any(indicator in ' '.join(topics) for indicator in intermediate_indicators):
            return 'high_school_college'
        
        # Default to middle school
        return 'middle_school'
    
    def _extract_subjects_from_topics(self, topics: list) -> list:
        """Extract main subjects from topics list"""
        subjects = set()
        for topic in topics:
            if '>' in topic:
                subject = topic.split('>')[0]
                subjects.add(subject)
        return list(subjects)
    
    def _assess_engagement_level(self, features: Dict[str, Any]) -> str:
        """Assess user engagement level"""
        conversations = features.get('conversations_7d', 0)
        frequency = features.get('frequency_7d', 0)
        
        if conversations > 50 and frequency > 100:
            return 'very_high'
        elif conversations > 20 and frequency > 50:
            return 'high'
        elif conversations > 5 and frequency > 10:
            return 'moderate'
        else:
            return 'low'
    
    def _determine_email_purpose(self, template_id: str, features: Dict[str, Any], ai_triggers: list) -> str:
        """Determine the main purpose of the email"""
        recency = features.get('recency_days', 0)
        engagement = self._assess_engagement_level(features)
        
        # Check AI triggers for specific purposes
        if ai_triggers:
            for trigger in ai_triggers:
                if isinstance(trigger, dict):
                    trigger_type = trigger.get('trigger_type', '')
                    if trigger_type == 'exam_followup':
                        return 'exam_followup'
                    elif trigger_type == 'learning_support':
                        return 'learning_support'
        
        # Determine purpose from user state
        if recency > 7:
            return 'winback'
        elif engagement == 'very_high':
            return 'engagement_reward'
        elif features.get('completed_courses', 0) > 0:
            return 'completion_celebration'
        elif features.get('test_accuracy', 0) > 0.8:
            return 'performance_praise'
        else:
            return 'learning_encouragement'
    
    def _generate_educational_subject(self, context: Dict[str, Any]) -> str:
        """Generate educational subject line"""
        purpose = context['email_purpose']
        subjects = context['user_profile']['primary_subjects']
        level = context['user_profile']['learning_level']
        
        subject_area = subjects[0] if subjects else 'your studies'
        
        if purpose == 'exam_followup':
            return f"How did your {subject_area} exam go? 📚"
        elif purpose == 'learning_support':
            return f"Boost your {subject_area} understanding! 🚀"
        elif purpose == 'winback':
            return f"Ready to continue your {subject_area} journey? 🎯"
        elif purpose == 'engagement_reward':
            return f"Amazing progress in {subject_area}! Keep it up! 🌟"
        elif purpose == 'completion_celebration':
            return f"Congratulations on your {subject_area} achievement! 🎓"
        elif purpose == 'performance_praise':
            return f"Excellent {subject_area} performance! 📈"
        else:
            return f"Your {subject_area} learning journey continues! 💪"
    
    def _generate_educational_fallback(self, context: Dict[str, Any]) -> str:
        """Generate educational fallback content"""
        purpose = context['email_purpose']
        subjects = context['user_profile']['primary_subjects']
        level = context['user_profile']['learning_level']
        engagement = context['user_profile']['engagement_level']
        
        subject_area = subjects[0] if subjects else 'studies'
        
        greeting = "Hi there, learner!" if level == 'middle_school' else "Hello, dedicated student!"
        
        if purpose == 'exam_followup':
            return f"""{greeting}

I hope your recent {subject_area} exam went well! Exams can be challenging, but they're great opportunities to see how much you've learned.

🎯 **Reflection Questions:**
• Which concepts felt most comfortable during the exam?
• Were there any topics that surprised you or felt more difficult?
• What study strategies worked best for your preparation?

Whether the exam went exactly as planned or not, remember that every test is a learning experience. I'm here to help you review any challenging concepts and prepare for what's next in your {subject_area} journey.

Keep up the great work!"""

        elif purpose == 'learning_support':
            return f"""{greeting}

I've noticed you've been diving deep into {subject_area} - that's fantastic! Your curiosity and dedication to learning are truly inspiring.

📚 **Your Recent Progress:**
• Active engagement in {subject_area} discussions
• Consistent study habits and practice
• Growing understanding of complex concepts

🚀 **Next Steps:**
Based on your learning patterns, here are some ways to keep building your {subject_area} expertise:
• Practice applying concepts to real-world scenarios
• Connect new learning to what you already know
• Don't hesitate to ask questions when something isn't clear

Remember, every expert was once a beginner. You're building strong foundations that will serve you well!"""

        elif purpose == 'winback':
            return f"""{greeting}

We miss seeing you in your {subject_area} studies! Life gets busy, and it's completely normal to take breaks from learning.

🎯 **Ready to jump back in?**
• Your previous progress is still there waiting for you
• We can start with a quick review to refresh your memory
• Small, consistent steps are better than trying to catch up all at once

Learning is a journey, not a race. Whether you've been away for a few days or a few weeks, there's no judgment here - just support for your continued growth.

What would you like to explore first when you're ready?"""

        elif purpose == 'engagement_reward':
            return f"""{greeting}

Your dedication to {subject_area} has been absolutely amazing! Your consistent engagement and thoughtful questions show real commitment to learning.

🌟 **What we've noticed:**
• Regular participation in {subject_area} discussions
• Thoughtful questions that show deep thinking
• Steady progress through challenging concepts

Your approach to learning is inspiring. Keep asking questions, stay curious, and remember that every challenge you overcome makes you stronger.

You're doing incredible work - keep it up!"""

        elif purpose == 'completion_celebration':
            return f"""{greeting}

Congratulations on completing your {subject_area} course! This is a significant achievement that represents hours of dedication, curiosity, and hard work.

🎓 **What you've accomplished:**
• Mastered key {subject_area} concepts
• Developed critical thinking skills
• Built confidence in tackling new challenges

🚀 **What's next?**
Your learning journey doesn't end here. Consider how you can apply what you've learned, explore related topics, or help others who are just starting their {subject_area} journey.

Celebrate this milestone - you've earned it!"""

        else:
            return f"""{greeting}

Your {subject_area} learning journey is unique and valuable. Every question you ask, every concept you explore, and every challenge you tackle contributes to your growth as a learner and thinker.

💪 **Keep in mind:**
• Learning is a process, not a destination
• Mistakes and confusion are normal parts of understanding
• Your curiosity and effort matter more than perfect answers

Whether you're just starting with {subject_area} or building on previous knowledge, remember that every expert was once where you are now.

Keep exploring, keep questioning, and keep growing!"""

    
    def get_available_templates(self) -> list:
        """Return empty list since we no longer use predefined templates"""
        return []
    
    def preview_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Return None since we generate content dynamically"""
        return None

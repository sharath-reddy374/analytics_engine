import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.orm import Session
from database.models import EmailSend, AppUser
from services.llm_service import LLMService
from typing import Dict, Any, List
import logging
from datetime import datetime
import json
from config.settings import settings

logger = logging.getLogger(__name__)

class EmailService:
    """
    Service for rendering and sending personalized emails
    """
    
    def __init__(self):
        self.sg = sendgrid.SendGridAPIClient(api_key=settings.SENDGRID_API_KEY) if settings.SENDGRID_API_KEY else None
        self.jinja_env = Environment(loader=FileSystemLoader('templates/email'))
        self.llm_service = LLMService()
        
        # Email templates mapping
        self.templates = {
            'bio_cells_help_v1': {
                'subject': '{{ first_name }}, a quick boost on Biology: Cells',
                'template': 'bio_cells_help.html',
                'content_links': {
                    'practice_link': '/practice/biology/cells',
                    'video_link': '/videos/biology/cell-basics'
                }
            },
            'algebra_next_steps_v1': {
                'subject': '{{ first_name }}, ready for the next algebra challenge?',
                'template': 'algebra_next_steps.html',
                'content_links': {
                    'practice_link': '/practice/algebra/variables',
                    'video_link': '/videos/algebra/distributive-property'
                }
            },
            'winback_study_plan_v1': {
                'subject': '{{ first_name }}, your personalized study plan is ready',
                'template': 'winback_study_plan.html',
                'content_links': {
                    'study_plan_link': '/dashboard/study-plan',
                    'quick_practice_link': '/practice/quick-review'
                }
            },
            'marketing_boost_v1': {
                'subject': '{{ first_name }}, master the 4 Ps of Marketing',
                'template': 'marketing_boost.html',
                'content_links': {
                    'practice_link': '/practice/marketing/4ps',
                    'case_study_link': '/case-studies/marketing-basics'
                }
            },
            'test_improvement_v1': {
                'subject': '{{ first_name }}, let\'s boost your test scores',
                'template': 'test_improvement.html',
                'content_links': {
                    'strategy_guide_link': '/guides/test-taking-strategies',
                    'practice_link': '/practice/adaptive-quiz'
                }
            }
        }
    
    async def send_campaign_emails(self, email_candidates: List[Dict[str, Any]], db: Session) -> Dict[str, int]:
        """
        Send emails to campaign candidates
        """
        results = {'sent': 0, 'failed': 0, 'skipped': 0}
        
        for candidate in email_candidates:
            try:
                success = await self._send_single_email(candidate, db)
                if success:
                    results['sent'] += 1
                else:
                    results['failed'] += 1
            except Exception as e:
                logger.error(f"Failed to send email to user {candidate['user_id']}: {str(e)}")
                results['failed'] += 1
        
        logger.info(f"Email campaign results: {results}")
        return results
    
    async def _send_single_email(self, candidate: Dict[str, Any], db: Session) -> bool:
        """Send a single personalized email"""
        
        user_id = candidate['user_id']
        template_id = candidate['template_id']
        
        # Get user details
        user = db.query(AppUser).filter(AppUser.user_id == user_id).first()
        if not user:
            logger.warning(f"User {user_id} not found")
            return False
        
        # Get template configuration
        template_config = self.templates.get(template_id)
        if not template_config:
            logger.warning(f"Template {template_id} not found")
            return False
        
        try:
            # Prepare template data
            template_data = await self._prepare_template_data(user, candidate, template_config)
            
            # Render email content
            subject = self._render_template_string(template_config['subject'], template_data)
            html_content = self._render_email_template(template_config['template'], template_data)
            
            # Send email
            if self.sg:
                success = await self._send_via_sendgrid(user.email, subject, html_content, candidate)
            else:
                # Log email for testing
                logger.info(f"Would send email to {user.email}: {subject}")
                success = True
            
            # Record email send
            self._record_email_send(user_id, template_id, subject, 'sent' if success else 'failed', candidate, db)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send email to {user.email}: {str(e)}")
            self._record_email_send(user_id, template_id, '', 'failed', candidate, db)
            return False
    
    async def _prepare_template_data(self, user: AppUser, candidate: Dict[str, Any], template_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for email template rendering"""
        
        features = candidate['features']
        top_topics = features.get('top_topics', [])
        primary_topic = top_topics[0] if top_topics else 'your studies'
        
        # Generate personalized LLM content
        llm_context = {
            'first_name': user.first_name or 'Student',
            'topic': primary_topic,
            'observed_struggle': self._infer_struggle_context(features),
            'tone': 'encouraging, concise'
        }
        
        llm_block = await self.llm_service.generate_email_content(llm_context)
        
        # Calculate expected gain
        expected_gain = self._calculate_expected_gain(features)
        
        template_data = {
            'first_name': user.first_name or 'Student',
            'topic': primary_topic,
            'practice_link': template_config['content_links'].get('practice_link', '/practice'),
            'video_link': template_config['content_links'].get('video_link', '/videos'),
            'study_plan_link': template_config['content_links'].get('study_plan_link', '/dashboard'),
            'expected_gain': expected_gain,
            'llm_block': llm_block,
            'unsubscribe_link': f'/unsubscribe?user_id={user.user_id}',
            'user_id': str(user.user_id),
            'frequency_7d': features.get('frequency_7d', 0),
            'tests_7d': features.get('tests_7d', 0),
            'churn_risk': features.get('churn_risk', 'low')
        }
        
        return template_data
    
    def _infer_struggle_context(self, features: Dict[str, Any]) -> str:
        """Infer learning struggle context from features"""
        
        if features.get('avg_score_change_30d', 0) < -0.1:
            return 'recent test performance challenges'
        elif features.get('convo_sentiment_7d_avg', 0) < -0.2:
            return 'learning frustration indicators'
        elif features.get('frequency_7d', 0) < 3:
            return 'maintaining consistent study habits'
        else:
            return 'continued learning progress'
    
    def _calculate_expected_gain(self, features: Dict[str, Any]) -> str:
        """Calculate expected learning gain message"""
        
        tests_7d = features.get('tests_7d', 0)
        frequency_7d = features.get('frequency_7d', 0)
        
        if tests_7d >= 5:
            return '15-20% score improvement'
        elif frequency_7d >= 7:
            return '10-15% better retention'
        else:
            return '5-10% knowledge boost'
    
    def _render_template_string(self, template_string: str, data: Dict[str, Any]) -> str:
        """Render a template string with data"""
        from jinja2 import Template
        template = Template(template_string)
        return template.render(**data)
    
    def _render_email_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """Render email template with data"""
        try:
            template = self.jinja_env.get_template(template_name)
            return template.render(**data)
        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {str(e)}")
            return self._get_fallback_email_content(data)
    
    def _get_fallback_email_content(self, data: Dict[str, Any]) -> str:
        """Fallback email content when template fails"""
        return f"""
        <html>
        <body>
            <h2>Hi {data.get('first_name', 'Student')}!</h2>
            <p>{data.get('llm_block', 'Keep up the great work on your learning journey!')}</p>
            <p>
                <a href="{data.get('practice_link', '/practice')}">Continue Practice</a> |
                <a href="{data.get('video_link', '/videos')}">Watch Video</a>
            </p>
            <p><small><a href="{data.get('unsubscribe_link', '/unsubscribe')}">Unsubscribe</a></small></p>
        </body>
        </html>
        """
    
    async def _send_via_sendgrid(self, to_email: str, subject: str, html_content: str, candidate: Dict[str, Any]) -> bool:
        """Send email via SendGrid"""
        try:
            from_email = Email(settings.FROM_EMAIL, settings.FROM_NAME)
            to_email_obj = To(to_email)
            content = Content("text/html", html_content)
            
            mail = Mail(from_email, to_email_obj, subject, content)
            
            # Add unsubscribe header
            mail.add_header({
                "List-Unsubscribe": f"<{settings.BASE_URL}/unsubscribe?user_id={candidate['user_id']}>"
            })
            
            response = self.sg.send(mail)
            
            if response.status_code in [200, 202]:
                logger.info(f"Email sent successfully to {to_email}")
                return True
            else:
                logger.error(f"SendGrid error {response.status_code}: {response.body}")
                return False
                
        except Exception as e:
            logger.error(f"SendGrid send failed: {str(e)}")
            return False
    
    def _record_email_send(self, user_id: str, template_id: str, subject: str, status: str, candidate: Dict[str, Any], db: Session):
        """Record email send in database"""
        
        email_record = EmailSend(
            user_id=user_id,
            template_id=template_id,
            subject=subject,
            status=status,
            meta={
                'rule_id': candidate['rule_id'],
                'priority': candidate['priority'],
                'features_snapshot': candidate['features']
            }
        )
        
        db.add(email_record)
        db.commit()

import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.getint('database', 'database_port')
DATABASE_USER = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')


INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_SESSION_TOKEN = parser.get('aws', 'aws_session_token')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')


BATCH_SIZE = parser.getint('etl_settings', 'batch_size', fallback=1000)
ERROR_HANDLING = parser.get('etl_settings', 'error_handling', fallback='log')
LOG_LEVEL = parser.get('etl_settings', 'log_level', fallback='INFO')

POST_FIELDS = (
    'id',
    'title',
    'selftext',
    'author',
    'subreddit',
    'score',
    'num_comments',
    'created_utc',
    'url',
    'upvote_ratio',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)

# {'comment_limit': 2048, 
#  'comment_sort': 'confidence', 
#  '_reddit': <praw.reddit.Reddit object at 0x7236b591eb80>, 
#  'approved_at_utc': None, 
#  'subreddit': Subreddit(display_name='lakers'), 
#  'selftext': '', 
#  'author_fullname': 't2_8jp21', 
#  'saved': False, 
#  'mod_reason_title': None, 
#  'gilded': 0, 
#  'clicked': False, 
#  'title': '[Charania] BREAKING: Marcus Smart has agreed to a contract buyout with the Washington Wizards and intends to sign a two-year, $11 million deal with the Los Angeles Lakers after clearing waivers, sources tell ESPN. A return to a grand stage for the 2022 NBA Defensive Player of the Year.', 
#  'link_flair_richtext': [], 
#  'subreddit_name_prefixed': 'r/lakers', 
#  'hidden': False, 
#  'pwls': 6, 
#  'link_flair_css_class': '',
#  'downs': 0,
#  'thumbnail_height': 140, 
#  'top_awarded_type': None, 
#  'hide_score': False, 'name': 't3_1m475p5', 
#  'quarantine': False, 
#  'link_flair_text_color': None, 
#  'upvote_ratio': 0.96, 
#  'author_flair_background_color': '#d3d6da', 
#  'ups': 2671, 
#  'total_awards_received': 0, 
#  'media_embed': {}, 
#  'thumbnail_width': 140, 
#  'author_flair_template_id': 'a851eae6-6a1a-11e5-bc6a-0ed4497f342d', 
#  'is_original_content': False, 
#  'user_reports': [], 
#  'secure_media': None, 
#  'is_reddit_media_domain': True, 
#  'is_meta': False, 
#  'category': None, 
#  'secure_media_embed': {}, 
#  'link_flair_text': 'BREAKING NEWS', 
#  'can_mod_post': False, 
#  'score': 2671, 
#  'approved_by': None, 
#  'is_created_from_ads_ui': False, 
#  'author_premium': False, 
#  'thumbnail': 'https://b.thumbs.redditmedia.com/w5LFUDvGY634rtJ2lcEV8ESKo23xZ3wCD0XkNeckPis.jpg', 
#  'edited': False, 
#  'author_flair_css_class': 'purple', 
#  'author_flair_richtext': [], 
#  'gildings': {}, 
#  'post_hint': 'image',
#    'content_categories': None, 
#    'is_self': False, 
#    'subreddit_type': 'public', 
#    'created': 1752959440.0, 
#    'link_flair_type': 'text', 
#    'wls': 6, 
#    'removed_by_category': None, 
#    'banned_by': None, 
#    'author_flair_type': 'text', 
#    'domain': 'i.redd.it', 
#    'allow_live_comments': False, 
#    'selftext_html': None, 
#    'likes': None, 
#    'suggested_sort': 
#    'confidence', 
#    'banned_at_utc': None, 
#    'url_overridden_by_dest': 'https://i.redd.it/amaljsmecwdf1.jpeg', 
#    'view_count': None, 
#    'archived': False, 
#    'no_follow': False, 
#    'is_crosspostable': False, 
#    'pinned': False, 
#    'over_18': False, 
#    'preview': {'images': [{'source': {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?auto=webp&s=0b09bf47f604da6a2679d7c2e7a67ac8db6a3b4f', 'width': 1290, 'height': 2154}, 'resolutions': [{'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=108&crop=smart&auto=webp&s=a416b6321d84a032083bc09e634a69e73ace3483', 'width': 108, 'height': 180},
#    {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=216&crop=smart&auto=webp&s=7b6af205fa31ec5c36354014e45d1dbb0097e2e5', 'width': 216, 'height': 360}, 
#    {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=320&crop=smart&auto=webp&s=b5d56723e39cdca397445b87882d65573c120a2f', 'width': 320, 'height': 534}, 
#    {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=640&crop=smart&auto=webp&s=b26b44bff9cf683e4e7a320d557f889bc3b56ba5', 'width': 640, 'height': 1068}, 
#    {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=960&crop=smart&auto=webp&s=aeba4b8cb898928b1a5c692e62fcce1ce4bf3346', 'width': 960, 'height': 1602}, 
#    {'url': 'https://preview.redd.it/amaljsmecwdf1.jpeg?width=1080&crop=smart&auto=webp&s=30a5e8045d4dbf3058ac173b1e9ce260e5568fe7', 'width': 1080, 'height': 1803}], 
#    'variants': {}, 
#    'id': 'FfBzMgGa3s29_uoroSPg7QgDHxCa2vraYKSePw226tI'}],
#    'enabled': True}, 
#    'all_awardings': [], 
#    'awarders': [], 
#    'media_only': False, 
#    'link_flair_template_id': '3a86f3ba-2ca4-11f0-be4b-0e8705606c1c', 
#    'can_gild': False, 
#    'spoiler': False, 
#    'locked': False, 
#    'author_flair_text': '24', 
#    'treatment_tags': [], 
#    'visited': False, 
#    'removed_by': None, 
#    'mod_note': None, 
#    'distinguished': None, 
#    'subreddit_id': 't5_2qhv6', 
#    'author_is_blocked': False, 
#    'mod_reason_by': None, 
#    'num_reports': None, 
#    'removal_reason': None, 
#    'link_flair_background_color': '#ffd635', 
#    'id': '1m475p5', 
#    'is_robot_indexable': True, 
#    'report_reasons': None, 
#    'author': Redditor(name='daftmunt'), 
#    'discussion_type': None, 
#    'num_comments': 673, 
#    'send_replies': False, 
#    'contest_mode': False, 
#    'mod_reports': [], 
#    'author_patreon_flair': False, 
#    'author_flair_text_color': 'dark',
#    'permalink': '/r/lakers/comments/1m475p5/charania_breaking_marcus_smart_has_agreed_to_a/', 
#    'stickied': False, 'url': 'https://i.redd.it/amaljsmecwdf1.jpeg', 
#    'subreddit_subscribers': 1607137, 
#    'created_utc': 1752959440.0, 'num_crossposts': 1, 'media': None, 'is_video': False, '_fetched': False, '_additional_fetch_params': {}, '_comments_by_id': {}}
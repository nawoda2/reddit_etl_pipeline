"""
Los Angeles Lakers 2024-2025 Roster Data
Contains player information for NER matching
"""

# Current Lakers roster as of 2024-2025 season
LAKERS_ROSTER = {
    # Guards
    "Luka Dončić": {
        "jersey_number": 77,
        "position": "Guard",
        "aliases": ["Luka", "Dončić", "Doncic", "Luka Doncic", "Luka Dončić"],
        "team_status": "Active"
    },
    "Austin Reaves": {
        "jersey_number": 15,
        "position": "Guard",
        "aliases": ["Austin", "Reaves", "AR15", "Austin Reaves"],
        "team_status": "Active"
    },
    "Marcus Smart": {
        "jersey_number": 36,
        "position": "Guard",
        "aliases": ["Marcus", "Smart", "Marcus Smart"],
        "team_status": "Active"
    },
    "Jordan Goodwin": {
        "jersey_number": 0,
        "position": "Guard",
        "aliases": ["Jordan", "Goodwin", "Jordan Goodwin"],
        "team_status": "Active"
    },
    "Bronny James": {
        "jersey_number": 9,
        "position": "Guard",
        "aliases": ["Bronny", "Bronny James", "Bronny Jr", "Bronny Jr."],
        "team_status": "Active"
    },
    "Gabe Vincent": {
        "jersey_number": 7,
        "position": "Guard",
        "aliases": ["Gabe", "Vincent", "Gabe Vincent"],
        "team_status": "Active"
    },
    
    # Forwards
    "LeBron James": {
        "jersey_number": 23,
        "position": "Forward",
        "aliases": ["LeBron", "James", "LeBron James", "King James", "LBJ", "Bron"],
        "team_status": "Active"
    },
    "Rui Hachimura": {
        "jersey_number": 28,
        "position": "Forward",
        "aliases": ["Rui", "Hachimura", "Rui Hachimura"],
        "team_status": "Active"
    },
    "Jake LaRavia": {
        "jersey_number": 3,
        "position": "Forward",
        "aliases": ["Jake", "LaRavia", "Jake LaRavia"],
        "team_status": "Active"
    },
    "Jarred Vanderbilt": {
        "jersey_number": 2,
        "position": "Forward",
        "aliases": ["Jarred", "Vanderbilt", "Jarred Vanderbilt", "Vando"],
        "team_status": "Active"
    },
    
    # Centers
    "Deandre Ayton": {
        "jersey_number": 5,
        "position": "Center",
        "aliases": ["Deandre", "Ayton", "Deandre Ayton", "DA"],
        "team_status": "Active"
    },
    "Jaxson Hayes": {
        "jersey_number": 11,
        "position": "Center",
        "aliases": ["Jaxson", "Hayes", "Jaxson Hayes"],
        "team_status": "Active"
    },
    "Maxi Kleber": {
        "jersey_number": 42,
        "position": "Center",
        "aliases": ["Maxi", "Kleber", "Maxi Kleber"],
        "team_status": "Active"
    },
    "Christian Koloko": {
        "jersey_number": 10,
        "position": "Center",
        "aliases": ["Christian", "Koloko", "Christian Koloko"],
        "team_status": "Active"
    }
}

# Create a comprehensive list of all possible name variations for matching
def get_all_player_aliases():
    """Get all possible aliases for all players"""
    all_aliases = []
    for player_name, player_info in LAKERS_ROSTER.items():
        all_aliases.extend([player_name] + player_info["aliases"])
    return list(set(all_aliases))  # Remove duplicates

def get_player_by_alias(alias):
    """Find the canonical player name by alias"""
    for player_name, player_info in LAKERS_ROSTER.items():
        if alias.lower() in [a.lower() for a in [player_name] + player_info["aliases"]]:
            return player_name
    return None

def get_roster_list():
    """Get list of all player names"""
    return list(LAKERS_ROSTER.keys())

def get_player_info(player_name):
    """Get player information by canonical name"""
    return LAKERS_ROSTER.get(player_name, None)

# Common misspellings and variations that might appear in Reddit posts
COMMON_VARIATIONS = {
    "lebron": "LeBron James",
    "lebron james": "LeBron James",
    "king james": "LeBron James",
    "lbj": "LeBron James",
    "bron": "LeBron James",
    "bronny": "Bronny James",
    "bronny james": "Bronny James",
    "bronny jr": "Bronny James",
    "bronny jr.": "Bronny James",
    "austin reaves": "Austin Reaves",
    "ar15": "Austin Reaves",
    "austin": "Austin Reaves",
    "reaves": "Austin Reaves",
    "luka": "Luka Dončić",
    "luka doncic": "Luka Dončić",
    "luka dončić": "Luka Dončić",
    "doncic": "Luka Dončić",
    "dončić": "Luka Dončić",
    "marcus smart": "Marcus Smart",
    "marcus": "Marcus Smart",
    "smart": "Marcus Smart",
    "rui": "Rui Hachimura",
    "rui hachimura": "Rui Hachimura",
    "hachimura": "Rui Hachimura",
    "vando": "Jarred Vanderbilt",
    "jarred vanderbilt": "Jarred Vanderbilt",
    "jarred": "Jarred Vanderbilt",
    "vanderbilt": "Jarred Vanderbilt",
    "deandre ayton": "Deandre Ayton",
    "deandre": "Deandre Ayton",
    "ayton": "Deandre Ayton",
    "da": "Deandre Ayton",
    "jaxson hayes": "Jaxson Hayes",
    "jaxson": "Jaxson Hayes",
    "hayes": "Jaxson Hayes",
    "gabe vincent": "Gabe Vincent",
    "gabe": "Gabe Vincent",
    "vincent": "Gabe Vincent",
    "jordan goodwin": "Jordan Goodwin",
    "jordan": "Jordan Goodwin",
    "goodwin": "Jordan Goodwin",
    "jake laravia": "Jake LaRavia",
    "jake": "Jake LaRavia",
    "laravia": "Jake LaRavia",
    "maxi kleber": "Maxi Kleber",
    "maxi": "Maxi Kleber",
    "kleber": "Maxi Kleber",
    "christian koloko": "Christian Koloko",
    "christian": "Christian Koloko",
    "koloko": "Christian Koloko"
}

def normalize_player_name(text):
    """Normalize player name from various formats to canonical name"""
    text_lower = text.lower().strip()
    
    # Check common variations first
    if text_lower in COMMON_VARIATIONS:
        return COMMON_VARIATIONS[text_lower]
    
    # Check against roster aliases
    for player_name, player_info in LAKERS_ROSTER.items():
        if text_lower in [a.lower() for a in [player_name] + player_info["aliases"]]:
            return player_name
    
    return None

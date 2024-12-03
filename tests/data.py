# Sample data


class Data:
    hero = {
        "id": 1,
        "name": "Erdrick",
        "class": "Hero",
        "level": 50,
        "equipment": {
            "weapon": "Erdrick's Sword",
            "armor": "Erdrick's Armor",
            "shield": "Erdrick's Shield",
            "helmet": "Erdrick's Helmet",
        },
    }

    monster = {
        "id": 2,
        "name": "Slime",
        "type": "Monster",
        "level": 1,
        "abilities": ["Fireball", "Radiant"],
    }

    equipment = {
        "id": 3,
        "name": "Zenithian Sword",
        "type": "Weapon",
        "attack": 120,
        "special_effect": "Disruptive Wave",
    }

    metal_slime = {
        "id": 4,
        "name": "Metal Slime",
        "type": "Monster",
        "level": 10,
        "abilities": [
            "Flee",
        ],
        "hp": 4,
        "defense": 9999,
    }

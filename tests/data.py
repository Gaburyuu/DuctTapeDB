# Sample data


class Data:
    hero = {
        "id": 1,
        "data": {
            "name": "Erdrick",
            "class": "Hero",
            "level": 50,
            "equipment": {
                "weapon": "Erdrick's Sword",
                "armor": "Erdrick's Armor",
                "shield": "Erdrick's Shield",
                "helmet": "Erdrick's Helmet",
            },
        },
    }

    monster = {
        "id": 2,
        "data": {
            "name": "Slime",
            "type": "Monster",
            "level": 1,
            "abilities": ["Fireball", "Radiant"],
        },
    }

    equipment = {
        "id": 3,
        "data": {
            "name": "Zenithian Sword",
            "type": "Weapon",
            "attack": 120,
            "special_effect": "Disruptive Wave",
        },
    }

    metal_slime = {
        "name": "Metal Slime",
        "type": "Monster",
        "level": 10,
        "abilities": [
            "Flee",
        ],
        "hp": 4,
        "defense": 9999,
    }

    monster_list = [
        {"name": "Slime", "type": "Monster", "level": 1, "hp": 5},
        {"name": "Metal Slime", "type": "Monster", "level": 10, "hp": 4},
        {"name": "Dragon", "type": "Monster", "level": 20, "hp": 50},
    ]

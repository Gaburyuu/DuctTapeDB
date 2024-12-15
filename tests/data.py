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
        {"name": "Slime", "type": "Slime", "level": 1, "hp": 5},
        {"name": "Metal Slime", "type": "Slime", "level": 10, "hp": 4},
        {"name": "King Slime", "type": "Slime", "level": 20, "hp": 50},
    ]

    monster_list_beasts = [
        {"name": "Bewarewolf", "type": "Beast", "level": 1, "hp": 5},
        {"name": "Scarewolf", "type": "Beast", "level": 10, "hp": 4},
        {"name": "Tearwolf", "type": "Beast", "level": 20, "hp": 50},
    ]

    monster_list_dragons = [
        {"name": "Tortagon", "type": "Dragon", "level": 1, "hp": 5},
        {"name": "Gasgon", "type": "Dragon", "level": 10, "hp": 4},
        {"name": "Coatal", "type": "Dragon", "level": 20, "hp": 50},
    ]

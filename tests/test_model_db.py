from src import DuctTapeModel, DuctTapeDB

# Create DB instances for each table in the same database file
slime_db = DuctTapeDB.create(table="slimes", path="monsters.db")
dragon_db = DuctTapeDB.create(table="dragons", path="monsters.db")


# Define models
class Slimes(DuctTapeModel):
    name: str
    power_level: int


class Dragons(DuctTapeModel):
    name: str
    fire_breathing: bool
    wing_span: float


# Set DBs for the models
Slimes.set_db(slime_db)
Dragons.set_db(dragon_db)

slime = Slimes(name="Great Slime", power_level=42)
# slime.save()

# Create and save a dragon
dragon = Dragons(name="Fire Dragon", fire_breathing=True, wing_span=15.5)
# dragon.save(

from src.python.database.Database import Database
from src.python.database.DbUtils import engine_HP, connection_SALON #connection_HP #,
#from src.python.database.DbUtils import connection_HP

base = "../data/"

def HP_neighbourhood():
    db = Database(connection_HP)
    db.create_neighbourhood_table("neighbourhood_hp")
    db.create_user_mapping_table("user_mapping_hp")
    db.create_counted_neighbours_table("counted_neighbours_hp")

    db.feed_neighbourhood_table("neighbourhood_hp")
    db.feed_user_mapping_table(engine_HP, f"{base}All_Labeled_users/HPMojeFixedPostActOldColumns__StandardScaler_k_7_labeled_users.csv",
                           "user_mapping_hp")
    db.feed_counted_neighbours_table("counted_neighbours_hp", "user_mapping_hp")
    db.create_counted_neighbours_table_new("counted_neighbours_hp_new")
    db.feed_counted_neighbours_table_new("counted_neighbours_hp_new", "user_mapping_hp")
    print("DONE feeding tables")

def SALON_neighbourhood():
    db = Database(connection_SALON)
    db.create_neighbourhood_table("neighbourhood")
    db.create_user_mapping_table("user_mapping")
    db.create_counted_neighbours_table("counted_neighbours")

    db.feed_neighbourhood_table("neighbourhood")
    db.feed_user_mapping_table(engine_HP, f"{base}All_Labeled_users/Salon24_new_analysis__StandardScaler_k_7_labeled_users.csv",
                           "user_mapping")
    db.feed_counted_neighbours_table("counted_neighbours", "user_mapping")

    db.create_counted_neighbours_table_new("counted_neighbours_new")
    db.feed_counted_neighbours_table_new("counted_neighbours_new", "user_mapping")
    print("DONE feeding tables")

if __name__ == '__main__':
    #HP_neighbourhood()
    SALON_neighbourhood()


import psycopg2
import pandas as pd
import os
import itertools
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters

# from src.python.clustering.ClusteringFeatures import hp_roles_dict
from src.python.database.Database import Database
from src.python.database.DbUtils import connection_HP
from src.python.utils.util import save_fig, save_data_to_file, cluster_dict, cluster_dict_num, \
    save_data_to_file_with_index

register_matplotlib_converters()

types_colors = {'0': "b", '1': "k", '2': "r", '3': "g", '4': "y", '5': "c", '6': "m"}


def get_cluster_neighbour_data(db, cluster, counted_neighbours, user_mapping):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH clustered_users_neighbours AS 
    (
    SELECT c.user_id, c.number_of_links, c.type, u.label as user_cluster, c.date 
    FROM {counted_neighbours} c 
    JOIN {user_mapping} u ON c.user_id = u.user_id AND c.date = u.date
    )
    
    SELECT AVG(number_of_links) as number_of_links, type, date 
    FROM clustered_users_neighbours WHERE user_cluster = '{cluster}' 
    GROUP BY user_cluster, type, date
    ORDER BY date
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    return res


def get_global_cluster_neighbour_data(db, counted_neighbours, user_mapping):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH clustered_users_neighbours AS 
    (
    SELECT c.user_id, c.number_of_links, c.type, u.label as user_cluster, c.date 
    FROM {counted_neighbours} c 
    JOIN {user_mapping} u ON c.user_id = u.user_id AND c.date = u.date
    )
    
    SELECT user_cluster, 
           type as neighbour_type, 
    AVG(number_of_links) AS average,
    STDDEV(number_of_links) AS standard_deviation,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY number_of_links) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY number_of_links) AS q3,
    MAX(number_of_links) AS max,
    MIN(number_of_links) AS min
    FROM clustered_users_neighbours
    GROUP BY user_cluster, type
    ORDER BY user_cluster, type
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    return res

def create_global_stats_neighbour(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    DROP TABLE global_stats_neighbour;

    CREATE TABLE global_stats_neighbour(
    user_cluster varchar(50),
    neighbour_type varchar(50),
    average decimal,
    standard_deviation decimal,
    median decimal,
    q3 decimal,
    max decimal,
    min decimal
    );
    """

    cur.execute(query)
    conn.commit()
    cur.close()

def get_global_cluster_neighbour_data_new(db, counted_neighbours, user_mapping, number_of_links_0):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH clustered_users_neighbours AS 
    (
    SELECT c.user_id, c.{number_of_links_0}, u.label as user_cluster, c.date 
    FROM {counted_neighbours} c 
    JOIN {user_mapping} u ON c.user_id = u.user_id AND c.date = u.date
    )
    
    INSERT INTO global_stats_neighbour
    SELECT user_cluster, 
           '{number_of_links_0}' as neighbour_type, 
    AVG({number_of_links_0}) AS average,
    STDDEV({number_of_links_0}) AS standard_deviation,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {number_of_links_0}) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {number_of_links_0}) AS q3,
    MAX({number_of_links_0}) AS max,
    MIN({number_of_links_0}) AS min
    FROM clustered_users_neighbours
    GROUP BY user_cluster
    ORDER BY user_cluster
    """
    cur.execute(query)
#    res = cur.fetchall()
    conn.commit()
    cur.close()


def plot_neighbours(db, counted_neighbours, user_mapping, folder, roles_dict):
    for clust in range(0, 7):
        data = []
        data.extend(get_cluster_neighbour_data(db, clust, counted_neighbours, user_mapping))
        df = pd.DataFrame(data, columns=["number_of_links", "type", "date"])
        ## mozna aggfunc first bo nie ma takich samych par type i data wiec mozna zawsze wziac pierwszy wynik
        cluser_neighbour_table = df.pivot_table(index='date', columns=['type'], values='number_of_links',
                                                aggfunc='first')
        cluser_neighbour_table = cluser_neighbour_table.fillna(0)

        neighbour_types = cluser_neighbour_table.columns.values

        cluser_neighbour_table = cluser_neighbour_table.reset_index()

        plt.figure(figsize=(20, 10))
        plt.rc('xtick',labelsize=20)
        plt.rc('ytick',labelsize=20)
        for t in neighbour_types:
            plt.plot('date', str(t), data=cluser_neighbour_table, marker='', markerfacecolor=types_colors.get(t), label=f"Role: {cluster_dict.get(t)}")
        # plt.yscale('symlog')
        plt.legend(prop={'size': 16})
        plt.title("Number of neighbours of {}".format(roles_dict.get(clust)), fontsize=20, fontweight='bold')
        save_fig(folder, f"neighbours_of_{roles_dict.get(clust)}", plt)
        # plt.savefig("NeighbourFigs/numbers_of_{}".format(hp_roles_dict.get(clust)))


def neighbours_picks_diffs_table(db, counted_neighbours, user_mapping, folder):
    result = pd.DataFrame()
    for clust in range(0, 7):
        data = []
        data.extend(get_cluster_neighbour_data(db, clust, counted_neighbours, user_mapping))
        df = pd.DataFrame(data, columns=["number_of_links", "type", "date"])
        ## mozna aggfunc first bo nie ma takich samych par type i data wiec mozna zawsze wziac pierwszy wynik
        cluser_neighbour_table = df.pivot_table(index='date', columns=['type'], values='number_of_links',
                                                aggfunc='first')
        cluser_neighbour_table = cluser_neighbour_table.fillna(0)

        all_neighbours = ['0', '1', '2', '3', '4', '5', '6']
        neighbour_types = cluser_neighbour_table.columns.values
        diff_neighbours = np.setdiff1d(all_neighbours, neighbour_types)

        cluser_neighbour_table = cluser_neighbour_table.reset_index()
        cluser_neighbour_table[neighbour_types] = cluser_neighbour_table[neighbour_types].diff()
        cluser_neighbour_table = cluser_neighbour_table.drop(cluser_neighbour_table.index[0])
        for dif in diff_neighbours:
            cluser_neighbour_table.insert(0, str(dif), 0)
        cluser_neighbour_table = cluser_neighbour_table.reindex(sorted(cluser_neighbour_table.columns), axis=1)
        cluser_neighbour_table = cluser_neighbour_table.drop(columns=["date"]).mean(axis=0)
        result["stats_of_" + str(clust)] = cluser_neighbour_table
    result.index.name = 'neighbour_type'
    save_data_to_file(folder, "picks_diffs_table", result.reset_index())


def global_statistics(db, counted_neighbours, user_mapping, folder, file):
    data = []
    data.extend(get_global_cluster_neighbour_data(db, counted_neighbours, user_mapping))
    df = pd.DataFrame(data,
                      columns=["cluster_role", "neighbour_type", "average", "standard_deviation", "median", "q3", "max",
                               "min"])
    save_data_to_file(folder, file, df)


def perform_page_rank(db):
    nodes(db)
    edges(db)
    outdegree(db)
    pagerank_exe(db)


def nodes(db, start_date = '2009-12-22', end_date = '2013-10-22'):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    DROP TABLE Node;

    CREATE TABLE Node(
    id int,
    date timestamp without time zone, PRIMARY KEY (id, date)
    );

    do $$
    declare
        declare start_date date := '{start_date}';
        declare end_date date := '{end_date}';
    begin
    while start_date < end_date loop
        insert into node (id, date) select distinct author_id, start_date from posts where date between start_date and start_date+ interval '28 day'
        union
        select distinct author_id, start_date from comments where date between start_date and start_date + interval '28 day';
        start_date := start_date + interval '14 day';
    end loop;
    end$$;

    """
    cur.execute(query)
    conn.commit()
    cur.close()


def edges(db, start_date = '2009-12-22', end_date = '2013-10-22'):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    DROP TABLE Edge;

    CREATE TABLE Edge(
    src int,
    dst int, 
    date timestamp without time zone NOT NULL,
    PRIMARY KEY (src, dst, date));

    do $$
    declare
        declare start_date date := '{start_date}';
        declare end_date date := '{end_date}';
    begin
    while start_date < end_date loop
        INSERT INTO Edge (src, dst, date) 
	    SELECT DISTINCT c.author_id as src, p.author_id as dst, start_date 
	    FROM posts p JOIN comments c ON p.id = c.post_id AND p.author_id <> c.author_id 
	    WHERE c.date between start_date AND start_date + interval '28 day' 
	    UNION 
	    SELECT DISTINCT c.author_id as src, pc.author_id as dst, start_date 
	    FROM comments pc JOIN comments c ON pc.id = c.parentcomment_id AND pc.author_id <> c.author_id 
	    WHERE c.date between start_date AND start_date + interval '28 day';
        start_date := start_date + interval '14 day';
    end loop;
    end$$;

    """
    cur.execute(query)
    conn.commit()
    cur.close()


def outdegree(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
        DROP TABLE OutDegree;
        CREATE TABLE OutDegree(
        id int,
        degree int,
        date timestamp without time zone NOT NULL,
        PRIMARY KEY (id, date)
        );

        do $$
        declare
            declare start_date date := '2009-12-22';
            declare end_date date := '2013-10-22';
        begin
        while start_date < end_date loop
            INSERT INTO OutDegree
            WITH
            nodes AS (SELECT * FROM Node WHERE date = start_date ),
            edges AS (SELECT * FROM Edge WHERE date = start_date)
            SELECT nodes.id, COUNT(edges.src), start_date
            FROM nodes LEFT OUTER JOIN edges
            ON nodes.id = edges.src
            GROUP BY nodes.id;
            start_date := start_date + interval '14 day';
        end loop;
        end$$;

        do $$
        declare
            declare total int;
        begin 
        total = (SELECT COUNT(*) FROM node); 
        UPDATE outdegree SET degree = total WHERE degree = 0;
        end$$;
    """
    cur.execute(query)
    conn.commit()
    cur.close()

def pagerank_exe(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
DROP TABLE PageRank;
DROP TABLE TmpRank;

CREATE TABLE PageRank(
id int, 
rank float,
date timestamp without time zone NOT NULL,
PRIMARY KEY(id, date)
);

CREATE TABLE TmpRank(
id int PRIMARY KEY,
rank float,
date timestamp without time zone NOT NULL
);

do $$
declare
   DECLARE ALPHA float := 0.85;
   DECLARE Node_Num int;
   DECLARE Iteration int = 0;
   DECLARE start_date date := '2009-12-22';
   DECLARE end_date date := '2013-10-22';
begin
   Node_Num = (SELECT COUNT(*) FROM node);

   while start_date < end_date loop

          --PageRank Init Value

       INSERT INTO PageRank
       WITH
         nodes AS (SELECT * FROM Node WHERE date = start_date),
         outdegrees AS (SELECT * FROM OutDegree WHERE date = start_date)
       SELECT nodes.id, (1 - ALPHA) / Node_Num, start_date
       FROM nodes INNER JOIN outdegrees
       ON nodes.id = outdegrees.id;

    while Iteration < 50 loop
            Iteration = Iteration + 1;

               INSERT INTO TmpRank
            WITH
               edges AS (SELECT * FROM Edge WHERE date = start_date ),
               outdegrees AS (SELECT * FROM OutDegree WHERE date = start_date),
               pageranks AS (SELECT * FROM PageRank WHERE date = start_date)

               SELECT edges.dst, SUM(ALPHA * pageranks.rank / outdegrees.degree) + (1 - ALPHA) / Node_Num, start_date
               FROM pageranks
               INNER JOIN edges ON pageranks.id = edges.src
               INNER JOIN outdegrees ON pageranks.id = outdegrees.id
               GROUP BY edges.dst;

               DELETE FROM PageRank WHERE date = start_date;
               INSERT INTO PageRank SELECT * FROM TmpRank;
               DELETE FROM TmpRank;

    end loop;

    start_date := start_date + interval '14 day';
   end loop;
end$$;

    """
    cur.execute(query)
    conn.commit()
    cur.close()

def get_page_rank_with_labels(db, page_rank, user_mapping):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH clustered_users_page_rank AS 
    (
    SELECT p.id as user_id, p.rank as rank, u.label as cluster_type, p.date 
    FROM {page_rank} p 
    JOIN {user_mapping} u ON p.id = u.user_id AND p.date = u.date
    )
    
    SELECT cluster_type,
    AVG(rank) AS average,
    STDDEV(rank) AS standard_deviation,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rank) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rank) AS q3,
    MAX(rank) AS max,
    MIN(rank) AS min
    FROM clustered_users_page_rank
    GROUP BY cluster_type
    ORDER BY cluster_type
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    return res

def get_page_rank_with_labels_normalized(db, page_rank, user_mapping):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH clustered_users_page_rank AS 
    (
    SELECT p.id as user_id, ((p.rank / MAX(p.rank) over ()) * 100) as rank, u.label as cluster_type, p.date 
    FROM {page_rank} p 
    JOIN {user_mapping} u ON p.id = u.user_id AND p.date = u.date
    )
    
    SELECT cluster_type,
    AVG(rank) AS average,
    STDDEV(rank) AS standard_deviation,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rank) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rank) AS q3,
    MAX(rank) AS max,
    MIN(rank) AS min
    FROM clustered_users_page_rank
    GROUP BY cluster_type
    ORDER BY cluster_type
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    return res

def page_rank_analysis(db, page_rank, user_mapping, folder):
    data = []
    data.extend(get_page_rank_with_labels(db, page_rank, user_mapping))
    df = pd.DataFrame(data,
                  columns=["cluster_type", "average_rank", "rank_standard_deviation", "median_rank", "q3_rank", "max_rank",
                           "min_rank"])
    save_data_to_file(folder, "page_rank_global_stats", df)

    data_normalized = []
    data_normalized.extend(get_page_rank_with_labels_normalized(db, page_rank, user_mapping))
    df_normalized = pd.DataFrame(data_normalized,
                  columns=["cluster_type", "average_rank", "rank_standard_deviation", "median_rank", "q3_rank", "max_rank",
                           "min_rank"])
    save_data_to_file(folder, "page_rank_global_stats_normalized", df_normalized)

def get_data(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
        SELECT * FROM global_stats_neighbour order by user_cluster, neighbour_type;
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data, columns=["user_cluster",
                                     "neighbour_type",
                                     "average",
                                     "standard_deviation",
                                     "median",
                                     "q3",
                                     "max",
                                     "min"])

    save_data_to_file_with_index("HP_global_stats", "global_stats", df)


def analyse_neighbour_data_HP():
    db = Database(connection_HP)
    try:
        plot_neighbours(db, "counted_neighbours_hp", "user_mapping_hp", "PLOT_HP_Neighbourhood_analysis", cluster_dict_num)
        print("plotting done...")
        neighbours_picks_diffs_table(db, "counted_neighbours_hp", "user_mapping_hp", "HP_Neighbourhood_analysis")
        print("neighbours picks done...")

        create_global_stats_neighbour(db)
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_0")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_1")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_2")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_3")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_4")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_5")
        get_global_cluster_neighbour_data_new(db, "counted_neighbours_hp_new", "user_mapping_hp", "number_of_links_6")
        print("DONE feeding global stats")

        get_data(db)

        perform_page_rank(db)
        print("page rank done...")
        page_rank_analysis(db, "PageRank", "user_mapping_hp", "HP_PageRank_analysis")
        print("page rank analysis done...")

    finally:
        db.conn.close()


        # global_statistics(db, "counted_neighbours_hp", "user_mapping_hp", "HP_Neighbourhood_analysis", "global_stats")
        # print("neighbours general done...")


if __name__ == "__main__":
    analyse_neighbour_data_HP()


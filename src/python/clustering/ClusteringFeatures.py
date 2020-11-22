import datetime

start_date_clustering = datetime.date(2009, 12, 22)
end_date_clustering = datetime.date(2013, 10, 22)

start_date_salon = datetime.date(2008, 1, 1)
end_date_salon = datetime.date(2013, 7, 6)

hp_roles_dict = {0: "0", 1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6"}

basic_columns = columns = ['start_date',
                           'user_id',
                           'posts_activity_time',
                           'frequency_of_posts_avg',  # number_of_posts
                           'frequency_of_posts_stddev',
                           'frequency_of_comments_q3',  # number_of_comments
                           'number_of_received_responses_to_post_max',
                           'number_of_received_responses_to_post_stddev',
                           'number_of_received_responses_to_post_avg',
                           'number_of_received_responses_to_post_q3',
                           'number_of_received_responses_under_comments_q3',
                           'number_of_received_responses_under_comments_max',
                           'number_of_own_post_responses_q3',
                           'number_of_words_in_posts_q3',
                           'number_of_words_in_comments_median',
                           'number_of_words_in_responses_to_posts_q3',
                           'number_of_words_in_own_post_responses_q3'] # avg zamias q3


basic_columns_salon = ['start_date',
                           'user_id',
                           'number_of_posts',
                           'number_of_comments',  # number_of_posts
                           'frequency_of_posts_stddev',
                           'frequency_of_comments_q3',
                           'frequency_of_comments_stddev',# number_of_comments
                           'number_of_received_responses_to_post_max',
                           'number_of_received_responses_to_post_q3',
                           'number_of_received_responses_under_comments_q3',
                           'number_of_received_responses_under_comments_max',
                           'number_of_own_post_responses_q3',
                           'number_of_words_in_posts_q3',
                           'number_of_words_in_comments_q3',
                           'number_of_words_in_responses_to_posts_q3',
                           'number_of_words_in_own_post_responses_q3'] # avg zamias q3


new_selected_columns1 = ['start_date',
 'user_id',
 'number_of_posts',
 'posts_activity_time',
 'number_of_comments',
 'frequency_of_posts_stddev',
 'frequency_of_comments_max', # potencjalnie do wywalenia
 'frequency_of_comments_min', # potencjalnie do wywalenia
 'number_of_received_responses_to_post_max',
 'number_of_received_responses_to_post_stddev',
 'number_of_received_responses_to_post_avg',
 'number_of_received_responses_to_post_q3',
 #'number_of_received_responses_under_comments_stddev',
 'number_of_received_responses_under_comments_q3',
 'number_of_received_responses_under_comments_max',
 'number_of_own_post_responses_q3',
 'number_of_own_post_responses_max', # potencjalnie do wywalenia ?
 'number_of_own_post_responses_stddev', # potencjalnie do wywalenia
 'number_of_words_in_posts_q3',
 'number_of_words_in_comments_avg',
 'number_of_words_in_responses_to_posts_avg',
 'number_of_words_in_own_post_responses_avg']

new_selected_columns2 = ['start_date',
                        'user_id',
                        'number_of_posts',
                        'posts_activity_time',
                        'number_of_comments',
                        'frequency_of_posts_stddev',
                        #'frequency_of_comments_max', # potencjalnie do wywalenia
                        #'frequency_of_comments_min', # potencjalnie do wywalenia
                        'number_of_received_responses_to_post_max',
                        'number_of_received_responses_to_post_stddev',
                        'number_of_received_responses_to_post_avg',
                        'number_of_received_responses_to_post_q3',
                         #'number_of_received_responses_under_comments_stddev',
                        'number_of_received_responses_under_comments_q3',
                        'number_of_received_responses_under_comments_max',
                        'number_of_own_post_responses_q3',
                        #'number_of_own_post_responses_max', # potencjalnie do wywalenia ?
                        #'number_of_own_post_responses_stddev', # potencjalnie do wywalenia
                        'number_of_words_in_posts_q3',
                        'number_of_words_in_comments_avg',
                        'number_of_words_in_responses_to_posts_avg',
                        'number_of_words_in_own_post_responses_avg']

new_selected_columns3 = ['start_date',
                        'user_id',
                        'number_of_posts',
                        'posts_activity_time',
                        'number_of_comments',
                        'frequency_of_posts_stddev',
                        #'frequency_of_comments_max', # potencjalnie do wywalenia
                        #'frequency_of_comments_min', # potencjalnie do wywalenia
                        'number_of_received_responses_to_post_max',
                        'number_of_received_responses_to_post_stddev',
                        'number_of_received_responses_to_post_avg',
                        'number_of_received_responses_to_post_q3',
                        #'number_of_received_responses_under_comments_stddev',
                        'number_of_received_responses_under_comments_q3',
                        'number_of_received_responses_under_comments_max',
                        'number_of_own_post_responses_q3',
                        #'number_of_own_post_responses_max', # potencjalnie do wywalenia ?
                        #'number_of_own_post_responses_stddev', # potencjalnie do wywalenia
                        'number_of_words_in_posts_q3',
                        'number_of_words_in_comments_avg',
                        'number_of_words_in_responses_to_posts_avg',
                        'number_of_words_in_own_post_responses_avg']

# basic_columns_new_corr = ['start_date',
#                           'user_id',
#                           'number_of_posts',  # frequency_of_posts_avg
#                           'posts_activity_time',
#                           'number_of_comments',  # frequency_of_comments_q3
#                           'frequency_of_posts_stddev',
#                           'frequency_of_comments_stddev',  # nie ma
#                           'number_of_received_responses_to_post_max',
#                           'number_of_received_responses_to_post_stddev',
#                           'number_of_received_responses_to_post_avg',
#                           'number_of_received_responses_under_comments_stddev',  # nie ma
#                           'number_of_received_responses_under_comments_q3',
#                           'number_of_received_responses_under_comments_max',
#                           'number_of_own_post_responses_max',
#                           'number_of_words_in_posts_q3',
#                           'number_of_words_in_comments_median',
#                           'number_of_words_in_responses_to_posts_q3',
#                           'number_of_words_in_own_post_responses_q3']

basic_columns_new_corr_fixed_act = ['start_date',
                                    'user_id',
                                    'number_of_posts',
                                    'posts_activity_time',
                                    'number_of_comments',
                                    'frequency_of_posts_stddev',
                                    'frequency_of_comments_stddev',
                                    'number_of_received_responses_to_post_max',
                                    'number_of_received_responses_to_post_stddev',
                                    'number_of_received_responses_to_post_avg',
                                    'number_of_received_responses_under_comments_stddev',
                                    'number_of_received_responses_under_comments_q3',
                                    'number_of_received_responses_under_comments_max',
                                    'number_of_own_post_responses_max',
                                    'number_of_words_in_posts_q3',
                                    'number_of_words_in_comments_median',
                                    'number_of_words_in_responses_to_posts_q3',
                                    'number_of_words_in_own_post_responses_q3']

columns_chosen_by_klaudia = ['start_date',
                             'user_id',
                             'number_of_posts',
                             'number_of_comments',
                             'posts_activity_time',
                             'frequency_of_posts_stddev',
                             'frequency_of_posts_q3',
                             'number_of_received_responses_to_post_avg',
                             'number_of_received_responses_to_post_stddev',
                             'number_of_received_responses_to_post_max',
                             'number_of_own_post_responses_q3',
                             'number_of_received_responses_under_comments_q3',
                             'number_of_received_responses_under_comments_max',
                             'frequency_of_comments_q3',
                             'number_of_words_in_comments_median',
                             'number_of_words_in_posts_avg',
                             'number_of_words_in_responses_to_posts_q3',
                             'number_of_words_in_own_post_responses_q3'
                             ]

columns_with_additional_new_feature = ['start_date',
                                       'user_id',
                                       'posts_activity_time',
                                       'frequency_of_posts_avg',
                                       'frequency_of_posts_stddev',
                                       'frequency_of_comments_stddev',
                                       'frequency_of_comments_q3',
                                       'number_of_received_responses_to_post_max',
                                       'number_of_received_responses_to_post_stddev',
                                       'number_of_received_responses_to_post_avg',
                                       'number_of_received_responses_under_comments_stddev',
                                       'number_of_received_responses_under_comments_q3',
                                       'number_of_received_responses_under_comments_max',
                                       'number_of_own_post_responses_max',
                                       'number_of_words_in_posts_q3',
                                       'number_of_words_in_comments_median',
                                       'number_of_words_in_responses_to_posts_q3',
                                       'number_of_words_in_own_post_responses_q3',
                                       'number_of_all_responses_from_unique_users_in_slot']

basic_columns_without_std_and_responses_q3 = columns = ['start_date',
                                                        'user_id',
                                                        'posts_activity_time',
                                                        'frequency_of_posts_avg',
                                                        'frequency_of_comments_q3',
                                                        'number_of_received_responses_to_post_max',
                                                        'number_of_received_responses_to_post_avg',
                                                        'number_of_received_responses_under_comments_q3',
                                                        'number_of_received_responses_under_comments_max',
                                                        'number_of_own_post_responses_q3',
                                                        'number_of_words_in_posts_q3',
                                                        'number_of_words_in_comments_median',
                                                        'number_of_words_in_responses_to_posts_q3',
                                                        'number_of_words_in_own_post_responses_q3', ]

basic_columns_modified = columns = ['start_date',
                                    'user_id',
                                    'number_of_posts',
                                    'number_of_comments',
                                    'posts_activity_time',
                                    'frequency_of_posts_avg',
                                    'frequency_of_comments_q3',
                                    'number_of_received_responses_to_post_max',
                                    'number_of_received_responses_to_post_avg',
                                    'number_of_received_responses_under_comments_q3',
                                    'number_of_received_responses_under_comments_max',
                                    'number_of_own_post_responses_q3',
                                    'number_of_words_in_posts_q3',
                                    'number_of_words_in_comments_median',
                                    'number_of_words_in_responses_to_posts_q3',
                                    'number_of_words_in_own_post_responses_q3', ]

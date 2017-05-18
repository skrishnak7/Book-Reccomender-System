from pyspark.mllib.recommendation import ALS
def averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1])) / nratings)

class RecommendationEngine:
    def __count_and_average_ratings(self):
        book_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        book_ID_with_avg_ratings_RDD = book_ID_with_ratings_RDD.map(averages)
        self.books_rating_counts_RDD = book_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,iterations=self.iterations, lambda_=self.regularization_parameter)

    def __predict_ratings(self, user_and_book_RDD):

        predicted_RDD = self.model.predictAll(user_and_book_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.books_titles_RDD).join(self.books_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        return predicted_rating_title_and_count_RDD

    def add_ratings(self, ratings):

        new_ratings_RDD = self.sc.parallelize(ratings)
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        self.__count_and_average_ratings()
        self.__train_model()
        return ratings

    def get_ratings_for_book_ids(self, user_id, book_ids):
        requested_books_RDD = self.sc.parallelize(book_ids).map(lambda x: (user_id, x))
        ratings = self.__predict_ratings(requested_books_RDD).collect()

        return ratings

    def get_top_ratings(self, user_id, books_count):
        user_unrated_books_RDD = self.books_RDD.filter(lambda rating: not rating[1] == user_id).map(lambda x: (user_id, x[0]))
        ratings = self.__predict_ratings(user_unrated_books_RDD).filter(lambda r: r[2] >= 300).takeOrdered(books_count,
                                                                                                           key=lambda
                                                                                                               x: -x[1])
        return ratings

    def __init__(self, sc, dataset_path):
        self.sc = sc
        logger.info("Reading Ratings Data")
        ratings_raw_RDD = self.sc.textFile("ratings_cleaned_10mil.csv")
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[7]), int(tokens[6]), float(tokens[4]))).cache()
        logger.info("Loading books data...")
        books_raw_RDD = self.sc.textFile("metadata_withid.csv")
        books_raw_data_header = books_raw_RDD.take(1)[0]
        self.books_RDD = books_raw_RDD.filter(lambda line: line != books_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[2]), tokens[4], tokens[3])).cache()
        self.books_titles_RDD = self.books_RDD.map(lambda x: (int(x[0]), x[1])).cache()
        self.__count_and_average_ratings()
        self.rank = 11
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model()
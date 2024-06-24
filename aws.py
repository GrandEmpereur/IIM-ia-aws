from boto3.session import Session
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Nom du profil AWS à utiliser
PROFILE_NAME = "dev"
TABLE_NAME = "Movies"


class DynamoDB:
    def __init__(self, table_name=TABLE_NAME, profile_name=PROFILE_NAME):
        # Exo 1 : Configurer Boto3 et accéder à la table `Movies`
        print(f"Initialisation de l'application et de la connexion à DynamoDB avec le profil {profile_name}")
        self._table_name = table_name
        self._session = Session(profile_name=profile_name)
        self.resource = self._session.resource('dynamodb')
        self.client = self.resource.meta.client
        self.table = self.resource.Table(table_name)

        # Vérification et création de la table si elle n'existe pas
        if self.check_table_exists():
            print(f"Table {self._table_name} existe déjà, suppression de la table...")
            self.delete_table()
        self.create_table_with_additional_gsi()

    def check_table_exists(self):
        try:
            self.client.describe_table(TableName=self._table_name)
            return True
        except self.client.exceptions.ResourceNotFoundException:
            return False

    def delete_table(self):
        try:
            self.table.delete()
            self.table.meta.client.get_waiter('table_not_exists').wait(TableName=self._table_name)
            print(f"\033[92mTable {self._table_name} supprimée avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la suppression de la table: {e}\033[0m")

    # Exo 16 : Ajouter un GSI sur `release_year` et `rating`
    def create_table_with_additional_gsi(self):
        try:
            print("Début de la création de la table avec un GSI sur release_year et rating...")
            self.table = self.resource.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {
                        'AttributeName': 'movie_id',
                        'KeyType': 'HASH'  # Clé de partition
                    },
                    {
                        'AttributeName': 'release_year',
                        'KeyType': 'RANGE'  # Clé de tri
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'movie_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'release_year',
                        'AttributeType': 'N'
                    },
                    {
                        'AttributeName': 'genre',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'rating',
                        'AttributeType': 'N'
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'GenreIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'genre',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'release_year',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    },
                    {
                        'IndexName': 'ReleaseYearRatingIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'release_year',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'rating',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            self.table.meta.client.get_waiter('table_exists').wait(
                TableName=self._table_name)
            print(
                f"\033[92mTable {self._table_name} créée avec succès avec GSIs sur genre et release_year-rating\033[0m")
        except self.client.exceptions.ResourceInUseException:
            print("\033[91mErreur lors de la création de la table avec GSIs: La table existe déjà\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la création de la table avec GSIs: {e}\033[0m")

    # Exo 3 : Insérer un film dans la table `Movies`
    def insert_movie(self, movie_id, title, release_year, genre, rating, details):
        try:
            print(f"Insertion du film {title} dans la table {self._table_name}...")
            self.table.put_item(
                Item={
                    'movie_id': movie_id,
                    'title': title,
                    'release_year': release_year,
                    'genre': genre,
                    'rating': Decimal(str(rating)),  # Utiliser Decimal pour les types flottants
                    'details': details
                }
            )
            print(f"\033[92mFilm {title} inséré avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'insertion du film {title}: {e}\033[0m")

    # Exo 4 : Insérer plusieurs films en utilisant `batch_writer`
    def insert_movies_batch(self, movies):
        try:
            print(f"Insertion de plusieurs films dans la table {self._table_name}...")
            with self.table.batch_writer() as batch:
                for movie in movies:
                    batch.put_item(
                        Item={
                            'movie_id': movie['movie_id'],
                            'title': movie['title'],
                            'release_year': movie['release_year'],
                            'genre': movie['genre'],
                            'rating': Decimal(str(movie['rating'])),  # Utiliser Decimal pour les types flottants
                            'details': movie['details']
                        }
                    )
            print("\033[92mTous les films ont été insérés avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'insertion de plusieurs films: {e}\033[0m")

    # Exo 5 : Récupérer un film par son `movie_id` et `release_year`
    def get_movie(self, movie_id, release_year):
        try:
            print(f"Récupération du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.get_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                }
            )
            item = response.get('Item')
            if item:
                print(f"\033[92mFilm trouvé: {item}\033[0m")
            else:
                print("\033[91mFilm non trouvé\033[0m")
            return item
        except Exception as e:
            print(f"\033[91mErreur lors de la récupération du film: {e}\033[0m")
            return None

    # Exo 6 : Rechercher des films par `genre` en utilisant le GSI
    def query_movies_by_genre(self, genre):
        try:
            print(f"Recherche des films du genre {genre}...")
            response = self.table.query(
                IndexName='GenreIndex',
                KeyConditionExpression=Key('genre').eq(genre)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films par genre {genre}: {e}\033[0m")
            return []

    # Exo 7 : Rechercher des films sortis après 2000
    def query_movies_by_release_year(self, year):
        try:
            print(f"Recherche des films sortis après l'année {year}...")
            response = self.table.scan(
                FilterExpression=Attr('release_year').gt(year)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films sortis après {year}: {e}\033[0m")
            return []

    # Exo 8 : Rechercher des films avec une note supérieure à 8.5
    def query_movies_by_rating(self, rating):
        try:
            print(f"Recherche des films avec une note supérieure à {rating}...")
            response = self.table.scan(
                FilterExpression=Attr('rating').gt(Decimal(str(rating)))
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films avec une note supérieure à {rating}: {e}\033[0m")
            return []

    # Exo 9 : Mettre à jour la note d'un film
    def update_movie_rating(self, movie_id, release_year, new_rating):
        try:
            print(f"Mise à jour de la note du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="set rating = :r",
                ExpressionAttributeValues={
                    ':r': Decimal(str(new_rating))
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mNote du film mise à jour avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la mise à jour de la note du film: {e}\033[0m")

    # Exo 10 : Supprimer un film de la table
    def delete_movie(self, movie_id, release_year):
        try:
            print(f"Suppression du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.delete_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                ReturnValues="ALL_OLD"
            )
            if 'Attributes' in response:
                print(f"\033[92mFilm supprimé avec succès: {response['Attributes']}\033[0m")
            else:
                print("\033[91mFilm non trouvé, rien à supprimer\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la suppression du film: {e}\033[0m")

    # Exo 11 : Ajouter un attribut JSON à un film
    def add_movie_awards(self, movie_id, release_year, awards):
        try:
            print(f"Ajout de l'attribut 'awards' au film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="set details.awards = :a",
                ExpressionAttributeValues={
                    ':a': awards
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mAwards ajoutés avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'ajout des awards au film: {e}\033[0m")

    # Exo 12 : Rechercher des films avec une durée supérieure à 150 minutes
    def query_movies_by_duration(self, min_duration):
        try:
            print(f"Recherche des films avec une durée supérieure à {min_duration} minutes...")
            response = self.table.scan(
                FilterExpression=Attr('details.duration').gt(min_duration)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films par durée: {e}\033[0m")
            return []

    # Exo 13 : Compter le nombre total de films dans la table
    def count_total_movies(self):
        try:
            print("Comptage du nombre total de films dans la table...")
            response = self.table.scan(
                Select='COUNT'
            )
            count = response['Count']
            print(f"\033[92mNombre total de films dans la table: {count}\033[0m")
            return count
        except Exception as e:
            print(f"\033[91mErreur lors du comptage des films: {e}\033[0m")
            return 0

    # Exo 14 : Rechercher des films par genre et année de sortie en utilisant une clé composite
    def query_movies_by_genre_and_year(self, genre, min_year):
        try:
            print(f"Recherche des films du genre {genre} sortis après l'année {min_year}...")
            response = self.table.query(
                IndexName='GenreIndex',
                KeyConditionExpression=Key('genre').eq(genre) & Key('release_year').gt(min_year)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films par genre et année: {e}\033[0m")
            return []

    # Exo 15 : Rechercher des films dont le titre commence par 'I'
    def query_movies_by_title_starting_with(self, prefix):
        try:
            print(f"Recherche des films dont le titre commence par '{prefix}'...")
            response = self.table.scan(
                FilterExpression=Attr('title').begins_with(prefix)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films par titre: {e}\033[0m")

    # Exo 17 : Rechercher des films par `release_year` en utilisant le nouveau GSI
    def query_movies_by_release_year_gsi(self, release_year):
        try:
            print(f"Recherche des films sortis en {release_year} en utilisant le GSI...")
            response = self.table.query(
                IndexName='ReleaseYearRatingIndex',
                KeyConditionExpression=Key('release_year').eq(release_year)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(
                f"\033[91mErreur lors de la recherche des films sortis en {release_year} en utilisant le GSI: {e}\033[0m")
            return []

    # Exo 18 : Rechercher des films avec une note supérieure à 8.5 en utilisant le nouveau GSI
    def query_movies_by_rating_gsi(self, release_year, rating):
        try:
            print(
                f"Recherche des films avec une note supérieure à {rating} en utilisant le GSI pour l'année {release_year}...")
            response = self.table.query(
                IndexName='ReleaseYearRatingIndex',
                KeyConditionExpression=Key('release_year').eq(release_year) & Key('rating').gt(Decimal(str(rating)))
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(
                f"\033[91mErreur lors de la recherche des films avec une note supérieure à {rating} en utilisant le GSI: {e}\033[0m")
            return []

    # Exo 19 : Mettre à jour les détails d'un film
    def update_movie_details(self, movie_id, release_year, new_details):
        try:
            print(f"Mise à jour des détails du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="set details = :d",
                ExpressionAttributeValues={
                    ':d': new_details
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mDétails du film mis à jour avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la mise à jour des détails du film: {e}\033[0m")

    # Exo 19.1 : Mettre à jour le champ `sequels` d'un film en augmentant sa valeur de 1
    def increment_movie_sequels(self, movie_id, release_year):
        try:
            print(f"Mise à jour du champ 'sequels' du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="SET details.sequels = details.sequels + :increment",
                ExpressionAttributeValues={
                    ':increment': Decimal(1)
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mChamp 'sequels' du film mis à jour avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la mise à jour du champ 'sequels' du film: {e}\033[0m")

    # Exo 20 : Supprimer tous les films d'un genre spécifique
    def delete_movies_by_genre(self, genre):
        try:
            print(f"Suppression de tous les films du genre {genre}...")
            response = self.table.query(
                IndexName='GenreIndex',
                KeyConditionExpression=Key('genre').eq(genre)
            )
            items = response.get('Items', [])
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(
                        Key={
                            'movie_id': item['movie_id'],
                            'release_year': item['release_year']
                        }
                    )
            print(f"\033[92mTous les films du genre {genre} ont été supprimés avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la suppression des films du genre {genre}: {e}\033[0m")

    # Exo 21 : Rechercher des films dont le réalisateur est "Christopher Nolan"
    def query_movies_by_director(self, director):
        try:
            print(f"Recherche des films réalisés par {director}...")
            response = self.table.scan(
                FilterExpression=Attr('details.director').eq(director)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films réalisés par {director}: {e}\033[0m")
            return []

    # Exo 22 : Rechercher des films avec une durée comprise entre 120 et 180 minutes
    def query_movies_by_duration_range(self, min_duration, max_duration):
        try:
            print(f"Recherche des films avec une durée comprise entre {min_duration} et {max_duration} minutes...")
            response = self.table.scan(
                FilterExpression=Attr('details.duration').between(min_duration, max_duration)
            )
            items = response.get('Items', [])
            for item in items:
                print(item)
            return items
        except Exception as e:
            print(
                f"\033[91mErreur lors de la recherche des films avec une durée comprise entre {min_duration} et {max_duration} minutes: {e}\033[0m")
            return []

    # Exo 23 : Ajouter des critiques dans le sous-objet `details` d'un film
    def add_movie_reviews(self, movie_id, release_year, reviews):
        try:
            print(f"Ajout des critiques au film avec ID {movie_id} et année de sortie {release_year}...")

            # Récupérer les critiques actuelles
            response = self.table.get_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                ProjectionExpression="details.reviews"
            )
            current_reviews = response.get('Item', {}).get('details', {}).get('reviews', [])

            # Vérifier si current_reviews est une liste, sinon initialiser comme liste
            if not isinstance(current_reviews, list):
                current_reviews = []

            # Ajouter les nouvelles critiques
            current_reviews.extend(reviews)

            # Mettre à jour les critiques
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="set details.reviews = :r",
                ExpressionAttributeValues={
                    ':r': current_reviews
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mCritiques ajoutées avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'ajout des critiques au film: {e}\033[0m")

    # Exo 23.1 : Ajouter une critique une par une dans le sous-objet `details` d'un film en utilisant list_append
    def add_single_review(self, movie_id, release_year, review):
        try:
            print(f"Ajout de la critique au film avec ID {movie_id} et année de sortie {release_year}...")

            # Mettre à jour les critiques en utilisant list_append
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="SET details.reviews = list_append(details.reviews, :r)",
                ExpressionAttributeValues={
                    ':r': [review]
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mCritique ajoutée avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'ajout de la critique au film: {e}\033[0m")

    # Exo 24 : Rechercher des films dont une critique contient le mot "Amazing"
    def query_movies_with_amazing_reviews(self):
        try:
            print(f"Recherche des films dont une critique contient le mot 'Amazing'...")
            response = self.table.scan(
                ProjectionExpression="movie_id, details.reviews"
            )
            items = response.get('Items', [])
            matching_movies = []

            for item in items:
                reviews = item.get('details', {}).get('reviews', [])
                for review in reviews:
                    if 'comment' in review and 'Amazing' in review['comment']:
                        matching_movies.append(item)
                        break  # Exit inner loop if a match is found

            for movie in matching_movies:
                print(movie)
            return matching_movies
        except Exception as e:
            print(f"\033[91mErreur lors de la recherche des films avec des critiques contenant 'Amazing': {e}\033[0m")
            return []

    # Exo 25 : Mettre à jour le champ `duration` d'un film en augmentant sa valeur de 10 minutes
    def increment_movie_duration(self, movie_id, release_year, increment):
        try:
            print(f"Mise à jour de la durée du film avec ID {movie_id} et année de sortie {release_year}...")
            response = self.table.update_item(
                Key={
                    'movie_id': movie_id,
                    'release_year': release_year
                },
                UpdateExpression="set details.#d = details.#d + :inc",
                ExpressionAttributeNames={
                    '#d': 'duration'
                },
                ExpressionAttributeValues={
                    ':inc': increment
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mDurée du film mise à jour avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la mise à jour de la durée du film: {e}\033[0m")

    # Exo 26 : Créer une table `Cinema` qui contient chacun les films et un nom
    def create_cinema_table(self):
        try:
            print("Vérification de l'existence de la table 'Cinema'...")
            if self.check_cinema_table_exists('Cinema'):
                print("La table 'Cinema' existe déjà, suppression en cours...")
                self.delete_cinema_table('Cinema')
                print("Table 'Cinema' supprimée")

            print("Début de la création de la table 'Cinema'...")
            self.resource.create_table(
                TableName='Cinema',
                KeySchema=[
                    {
                        'AttributeName': 'cinema_id',
                        'KeyType': 'HASH'  # Clé de partition
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'cinema_id',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            self.resource.meta.client.get_waiter('table_exists').wait(TableName='Cinema')
            print("\033[92mTable 'Cinema' créée avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la création de la table 'Cinema': {e}\033[0m")

    def check_cinema_table_exists(self, table_name):
        try:
            self.resource.meta.client.describe_table(TableName=table_name)
            return True
        except self.resource.meta.client.exceptions.ResourceNotFoundException:
            return False

    def delete_cinema_table(self, table_name):
        try:
            table = self.resource.Table(table_name)
            table.delete()
            self.resource.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
            print(f"\033[92mTable '{table_name}' supprimée avec succès\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de la suppression de la table '{table_name}': {e}\033[0m")

    def add_movies_to_cinema(self, cinema_id, movie_ids):
        try:
            print(f"Ajout des films au cinéma avec ID {cinema_id}...")
            cinema_table = self.resource.Table('Cinema')
            response = cinema_table.update_item(
                Key={
                    'cinema_id': cinema_id
                },
                UpdateExpression="SET films = :f",
                ExpressionAttributeValues={
                    ':f': movie_ids
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"\033[92mFilms ajoutés au cinéma avec succès: {response['Attributes']}\033[0m")
        except Exception as e:
            print(f"\033[91mErreur lors de l'ajout des films au cinéma: {e}\033[0m")

    def get_all_movie_ids(self):
        try:
            print("Récupération de tous les IDs de films...")
            response = self.table.scan(
                ProjectionExpression="movie_id"
            )
            movie_ids = [item['movie_id'] for item in response['Items']]
            return movie_ids
        except Exception as e:
            print(f"\033[91mErreur lors de la récupération des IDs de films: {e}\033[0m")
            return []


def main():
    db = DynamoDB()

    # Exo 3 : Insertion d'un film
    db.insert_movie(
        movie_id="uuid-1",
        title="Inception",
        release_year=2010,
        genre="Sci-Fi",
        rating=Decimal('8.8'),  # Utiliser Decimal pour les types flottants
        details={"director": "Christopher Nolan", "duration": 148}
    )

    # Exo 4 : Insertion de plusieurs films
    movies = [
        {
            'movie_id': "uuid-2",
            'title': "The Matrix",
            'release_year': 1999,
            'genre': "Action",
            'rating': Decimal('8.7'),
            'details': {"director": "Wachowski", "duration": 136}
        },
        {
            'movie_id': "uuid-3",
            'title': "Interstellar",
            'release_year': 2014,
            'genre': "Sci-Fi",
            'rating': Decimal('8.6'),
            'details': {"director": "Christopher Nolan", "duration": 169}
        }
    ]
    db.insert_movies_batch(movies)

    # Exo 5 : Récupération d'un film
    db.get_movie('uuid-1', 2010)

    # Exo 6 : Recherche des films par genre
    db.query_movies_by_genre('Sci-Fi')

    # Exo 7 : Recherche des films sortis après 2000
    db.query_movies_by_release_year(2000)

    # Exo 8 : Recherche des films avec une note supérieure à 8.5
    db.query_movies_by_rating(8.5)

    # Exo 9 : Mettre à jour la note du film "Inception"
    db.update_movie_rating('uuid-1', 2010, 9.0)

    # Exo 10 : Supprimer le film "The Matrix"
    db.delete_movie('uuid-2', 1999)

    # Exo 11 : Ajouter un attribut "awards" au film "Inception"
    db.add_movie_awards('uuid-1', 2010, {"oscars": 4})

    # Exo 12 : Rechercher des films avec une durée supérieure à 150 minutes
    db.query_movies_by_duration(150)

    # Exo 13 : Compter le nombre total de films dans la table
    db.count_total_movies()

    # Exo 14 : Rechercher des films du genre "Sci-Fi" sortis après l'année 2000
    db.query_movies_by_genre_and_year('Sci-Fi', 2000)

    # Exo 15 : Rechercher des films dont le titre commence par 'I'
    db.query_movies_by_title_starting_with('I')

    # Exo 17 : Rechercher des films par `release_year` en utilisant le nouveau GSI
    db.query_movies_by_release_year_gsi(2014)

    # Exo 18 : Rechercher des films avec une note supérieure à 8.5 en utilisant le nouveau GSI
    db.query_movies_by_rating_gsi(2014, 8.5)

    # Exo 19 : Mettre à jour les détails du film "The Matrix"
    db.update_movie_details("uuid-2", 1999, {"director": "Wachowski", "duration": 136, "sequels": 2})

    # Exo 19.1 : Mettre à jour le champ `sequels` d'un film en augmentant sa valeur de 1
    db.increment_movie_sequels("uuid-2", 1999)

    # Exo 20 : Supprimer tous les films d'un genre spécifique
    db.delete_movies_by_genre('Action')

    # Exo 21 : Rechercher des films dont le réalisateur est "Christopher Nolan"
    db.query_movies_by_director("Christopher Nolan")

    # Exo 22 : Rechercher des films avec une durée comprise entre 120 et 180 minutes
    db.query_movies_by_duration_range(120, 180)

    # Exo 23 : Ajouter plusieurs critiques d'un coup dans le sous-objet `details` d'un film
    reviews = [
        {"reviewer": "Alice", "comment": "Great movie!"},
        {"reviewer": "Bob", "comment": "Loved it!"}
    ]
    db.add_movie_reviews("uuid-1", 2010, reviews)

    # Exo 23.1 : Ajouter des critiques une par une dans le sous-objet `details` d'un film
    db.add_single_review("uuid-1", 2010, {"reviewer": "Charlie", "comment": "Amazing"})
    db.add_single_review("uuid-1", 2010, {"reviewer": "Dave", "comment": "Amazing!"})

    # Exo 24 : Rechercher des films dont une critique contient le mot "Amazing"
    db.query_movies_with_amazing_reviews()

    # Exo 25 : Mettre à jour le champ `duration` d'un film en augmentant sa valeur de 10 minutes
    db.increment_movie_duration("uuid-1", 2010, 10)

    # Exo 26 : Créer une table `Cinema` et ajouter les films
    db.create_cinema_table()
    movie_ids = db.get_all_movie_ids()
    db.add_movies_to_cinema('cinema-1', movie_ids)
    db.add_movies_to_cinema('cinema-2', ['uuid-1', 'uuid-3'])

    print("\033[92mFin de l'application\033[0m")


# Exemple d'utilisation
if __name__ == "__main__":
    main()

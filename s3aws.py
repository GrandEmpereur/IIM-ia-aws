import boto3
import requests
from botocore.exceptions import NoCredentialsError

# Nom du profil AWS à utiliser
PROFILE_NAME = "dev"

# Configuration de Boto3
session = boto3.Session(profile_name=PROFILE_NAME)
s3 = session.client('s3')


def upload_file_to_s3(url, bucket_name, s3_file_name):
    try:
        # Télécharger le fichier depuis l'URL
        response = requests.get(url)
        response.raise_for_status()  # Vérifie si la requête a réussi

        # Envoyer le fichier à S3
        s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=response.content)
        print(f"\033[92mFichier téléchargé avec succès de {url} vers {bucket_name}/{s3_file_name}\033[0m")
    except requests.exceptions.RequestException as e:
        print(f"\033[91mErreur lors du téléchargement du fichier depuis l'URL: {e}\033[0m")
    except NoCredentialsError:
        print("\033[91mErreur: Identifiants AWS non trouvés\033[0m")
    except Exception as e:
        print(f"\033[91mErreur lors de l'envoi du fichier à S3: {e}\033[0m")


def main():
    url = "https://via.placeholder.com/300x300"  # URL de l'image placeholder 300x300
    bucket_name = "s3-aws-123"  # Remplacez par le nom de votre bucket S3
    s3_file_name = "placeholder_image_300x300.png"  # Nom du fichier dans S3

    upload_file_to_s3(url, bucket_name, s3_file_name)


if __name__ == "__main__":
    main()

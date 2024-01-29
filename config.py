import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Acces environment variables
DATABASE_URL = os.getenv("DEVELOPMENT_DATABASE_URL")


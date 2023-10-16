import logging
from sqlalchemy import create_engine, Column, Integer, String, LargeBinary, \
    UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import hashlib
import subprocess
import platform
from urllib.parse import urlparse, unquote


Base = declarative_base()
logger = logging.getLogger(__file__)


class FileRecord(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String)
    checksum = Column(String, unique=True)
    content = Column(LargeBinary)
    file_size = Column(Integer)

    __table_args__ = (UniqueConstraint('checksum'),)


class FileHandler:
    def __init__(self, db_url="sqlite:///files.db"):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def get_filename_from_url(self, url):
        parsed_url = urlparse(url)
        filename = unquote(parsed_url.path.split('/')[-1])
        return filename

    def fetch_file_from_url(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        else:
            logger.info(f"Failed to download file from {url}")
            return None

    def calculate_checksum(self, file_content):
        return hashlib.sha256(file_content).hexdigest()

    def save_file_to_db(self, filename, file_content):
        session = self.Session()
        checksum = self.calculate_checksum(file_content)
        file_size = len(file_content)

        existing_file = session.query(FileRecord).filter_by(checksum=checksum).first()
        if existing_file:
            logger.info(f"File with the same checksum ({checksum}) already exists.")
            return

        new_file = FileRecord(filename=filename,
                              checksum=checksum,
                              content=file_content,
                              file_size=file_size)
        session.add(new_file)
        session.commit()

    def get_file_from_db(self, filename):
        session = self.Session()
        file_record = session.query(FileRecord).filter_by(filename=filename).first()
        return file_record.content if file_record else None

    def open_with_default_program(self, filename):
        extracted_content = self.get_file_from_db(filename)
        if extracted_content:
            with open(filename, 'wb') as f:
                f.write(extracted_content)

            system_platform = platform.system()
            if system_platform == "Windows":
                subprocess.run(f'start {filename}', shell=True)
            elif system_platform == "Darwin":
                subprocess.run(f'open {filename}', shell=True)
            elif system_platform == "Linux":
                subprocess.run(f'xdg-open {filename}', shell=True)
        else:
            logger.info(f'No such file found! ({filename})')


if __name__ == "__main__":
    handler = FileHandler()

    # Fetch file from URL
    file_url = "https://www.liga.net/images/general/2023/09/28/20230928212258-7306.jpg"
    file_content = handler.fetch_file_from_url(file_url)
    if file_content:
        filename = handler.get_filename_from_url(file_url)
        # Save the file to the database
        handler.save_file_to_db(filename, file_content)

        # Open the file using the default program
        handler.open_with_default_program(filename)

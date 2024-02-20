
import requests

def fetch_data_with_retry(url, max_retries=3):
    retries = 0

    while retries < max_retries:
        try:
            # Set a user-agent header to simulate a web browser
            headers = {'User-Agent': 'YourUserAgent'}

            # Make the request using requests library
            response = requests.get(url, headers=headers)

            # Check for 429 status code
            if response.status_code == 429:
                # Check if Retry-After header is present
                if 'Retry-After' in response.headers:
                    retry_after = response.headers['Retry-After']
                    print(f"Got 429. Retry-After: {retry_after} seconds.")
                else:
                    print("Got 429, but Retry-After header is not present.")

                # Process the Retry-After information as needed
                # ...

                return None  # You may want to return or raise an exception based on your needs

            # Process the response as needed
            data = response.json()  # Adjust based on the actual response format
            return data

        except Exception as e:
            print(f"Error: {e}")
            retries += 1

    print("Max retries reached. Unable to fetch data.")
    return None

# Example usage
url_to_scrape = 'https://www.hockey-reference.com/boxscores/202310100TBL.html'
fetch_data_with_retry(url_to_scrape)

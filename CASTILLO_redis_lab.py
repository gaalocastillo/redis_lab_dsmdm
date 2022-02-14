# =====================================================================================================================
# Created By  : Galo Castillo
# Created Date: Mon February 14, 2022
# Description: Redis lab work of the Distributed Systems for Massive Data Management at University of Paris-Saclay 
# =====================================================================================================================

import redis
import hashlib

"""
    In summary, the structure of the data storage is the following:
    - One key is stored per user with the following format: user.<email>
    - Each key per user contains a dicionary as value, where each key is a long URL and its value is its short version.
    - In addition, a key called 'short_urls.count' has short URLs as keys and counters of requests as value.
    - Finally, a key called 'short_urls.tolong' contains short URLs as keys and the long version of the URL as value.

    In the main manu you will find the following functionalities:
    - Look how many requests have been made for each short URL.
    - Given a short URL, request its long version.
    - Authenticate (any email is accepted, previously used or not)

    In the authentication manu you will find the following functionalities:
    - Show how many URLs have been shortened by the authenticated user.
    - Given a long URL, request to create a shortened version if it does not exist.
    - Logout and return to the main menu.
"""

# Connector to redis
client = redis.Redis(host='localhost', port=6379)

"""
    These constant values correspond to part of all the keys that will be stored.
"""
KEY_SHORT_URLS_COUNT = 'short_urls.count'
KEY_SHORT_TO_LONG_URLS = 'short_urls.tolong'
KEY_USERS = 'users.'

"""
    These constant values correspond to the commands the user can use.
"""
GET_STATISTICS_SHORT_URLS_INPUT = 'get_statistics_short_urls'
GET_LONG_FROM_SHORT_URLS_INPUT = 'get_long_from_short_url'
GET_STATISTICS_USER_INPUT = 'get_statistics_user'
SHORTEN_INPUT = 'shorten'
EXIT_INPUT = 'exit'
LOGOUT_INPUT = 'logout'

# We provide a shortened URL using the first K_FIRST characters returned by a hashing function.
K_FIRST = 8

# This is the base URL for the shortened URL versions.
BASE_URL = 'https://galo/'

GOODBYE_MSG = "\n Good bye"


flag_system = True
while flag_system:

    flag_user = True
    
    print ("""
    Write the command 'AUTH <email>' email for authentication to register create your own short URLs.
    Write the command 'GET_STATISTICS_SHORT_URLS' to show how many requests have been made for each short URL.
    Write the command 'GET_LONG_FROM_SHORT_URL <short_url>' to show the long version of the given URL.
    Write the command 'EXIT' to exit the application.
    """)

    ans = str(input("")) 
    
    # Implementation for exiting the program.
    if ans.strip().lower() == EXIT_INPUT: 
      flag_system = False

    # Implementation for showing how many times the each short URL haves been requested.
    # Task c1 of the lab.
    elif len(ans.strip().split()) == 1 and ans.strip().lower().split()[0] == GET_STATISTICS_SHORT_URLS_INPUT:
        short_urls_count = client.hgetall(KEY_SHORT_URLS_COUNT)
        if len(short_urls_count) == 0:
            print('There are no short urls created!\n')
        else:
            for k in short_urls_count:
                v = int(short_urls_count[k])
                print('The short URL %s has been requested %d times.' % (str(k.decode('utf-8')), v))
            print()

    # Implementation for showing the long version of a given short url.
    # Task b of the lab.
    elif len(ans.strip().split()) == 2 and ans.strip().lower().split()[0] == GET_LONG_FROM_SHORT_URLS_INPUT:
        # First I check there are at least one URL stored.
        short_urls_to_long = client.hgetall(KEY_SHORT_TO_LONG_URLS)
        if len(short_urls_to_long) == 0:
            print('There are no short URLs created!\n')
        else:
            short_url_input = ans.strip().lower().split()[1]
            # Check if the shortened URL exists.
            long_url = client.hget(KEY_SHORT_TO_LONG_URLS, short_url_input)
            if long_url == None:
                print('The short URL does not exist!')
            else:
                # If it exists, its long versions is retrieved in the variable 'long_url'.
                print('The long version of %s is: %s' % (str(short_url_input), str(long_url.decode())))

                # After requesting the long URL, I increease in 1 the counter of requests for the given short URL.
                client.hincrby(KEY_SHORT_URLS_COUNT, short_url_input, 1)

    # Implementation for performing functionalities related to users and authentication.
    elif len(ans.strip().split()) == 2 and ans.strip().lower().split()[0] == 'auth':
        email = ans.strip().lower().split()[1]
        print('Welcome,', email)
        flag_user = True
        while flag_user:
            print ("""
            Write the command 'GET_STATISTICS_USER' to show how many URLs you have inserted.
            Write the command 'SHORTEN <url>' to create a shortened version of a long URL.
            Write the command 'LOGOUT' to logout the application.
            Write the command 'EXIT' to exit the application.
            """)

            ans_log = input("")

            # Implementation for logout the user account.
            if ans_log.strip().lower() == LOGOUT_INPUT: 
                flag_user = False

            # Implementation for exiting the program.
            elif ans_log.strip().lower() == EXIT_INPUT: 
                flag_user = False
                flag_system = False

            # Implementation for showing how many urls have been stored by the user.            
            elif len(ans_log.strip().split()) == 1 and ans_log.strip().lower().split()[0] == GET_STATISTICS_USER_INPUT:
                # First, I check that there are URLs stored for the given email/user.
                user_data = client.hgetall(KEY_USERS + email)
                if len(user_data) == 0:
                    print('There are no URLS inserted by you.')
                else:
                    # If the user exists, then I get the size of its value (dictionary, where keys are the long URLs s/he hass requested to shorten).
                    urls_count = len(user_data)
                    print('You have added %d URLs' % urls_count)

            # Implementation for shorting a standard url.            
            elif len(ans_log.strip().split()) == 2 and ans_log.strip().lower().split()[0] == SHORTEN_INPUT:
                long_url_input = ans_log.strip().lower().split()[1]
                # The shortened version of a URL is based on hashing the email and long URL.
                long_url_email = long_url_input + email
                long_url_email = long_url_email.encode()
                
                # First, I check if the user has already shortened that URL.
                short_url = client.hget(KEY_USERS + email, long_url_input)

                if short_url == None:
                    # If the user has not shortened the long URL before, then I create it.
                    # The URL is based on a base URL and the first K_FIRST (8) characters of a hashing function at the end as explained before.
                    short_url = BASE_URL + str(hashlib.sha256(long_url_email).hexdigest()[:K_FIRST])
                    
                    # I create the new shortened URL in the authenticated user information.
                    client.hset(KEY_USERS + email, long_url_input, short_url)

                    # Then I set the requests counter for that shortened URL as zero.
                    client.hset(KEY_SHORT_URLS_COUNT, short_url, 0)

                    # Finally, I store a mapping of the short URL to the long URL.
                    client.hset(KEY_SHORT_TO_LONG_URLS, short_url, long_url_input)
                    print('Your url has been successfully shortened. The short URL is:', str(short_url))
                else:
                    # If the shortened URL already exists, return the existing short URL.
                    print('Your URL was already shortened before. The short URL is:', short_url.decode('utf-8'))
            else:
                print('Please write a valid command.')
            print()
    else:
        if flag_user:
            print('Please write a valid command.')
    print()

print(GOODBYE_MSG)
exit()

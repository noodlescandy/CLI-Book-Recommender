import numpy as np
import warnings, multiprocessing as mp
warnings.simplefilter(action='ignore', category=FutureWarning)
import ctypes, pandas as pd

PATH = '../data/'
# Example exports to try, can also put an export in the data folder and try your own.
EXPORT = 'small_goodreads_library_export.csv' # more narrow interests
#EXPORT = 'small2_goodreads_library_export.csv' # broader interests, less specific recs
#EXPORT = 'medium_goodreads_library_export.csv'
#EXPORT = 'large_goodreads_library_export.csv'

def get_length(arr):
    c = 0
    for a in arr:
        c+=1
        if a == -1:
            return c
    return c

# looks up all books in the dataframe (user) against id_map, returns dictionary with user_ids and counts of similar books
def __lookup_books(user):
    global id_map # copied on creation
    similar_readers = {}
    user_csv_ids = []
    for index,row in user.iterrows():
        id_index = id_map.loc[(id_map['book_id'] == row['Book Id']), 'book_id_csv']
        if id_index.empty: # not in dataset
            print("Could not match id", row['Book Id'], "- removing book")
            user.drop(index) # remove unfound id
            index-=1
        else: # found it
            book_id = int(id_index.iloc[0])
            user_csv_ids.append(book_id)
            readers = fun.getListUsers(book_id).contents
            print(f"Book {book_id} has {get_length(readers)} other readers.")
            # add users to similar_readers dict
            for reader in readers:
                if reader == -1:
                    break
                if reader in similar_readers:
                    similar_readers[reader] += 1
                else:
                    similar_readers[reader] = 0
    return [similar_readers, user_csv_ids]

# load c functions
fun = ctypes.CDLL("./libbook.so")
fun.getListUsers.argtypes = [ctypes.c_int]
fun.getListUsers.restype = ctypes.POINTER(ctypes.c_int * 877000)
fun.getListBooks.restype = ctypes.POINTER(ctypes.c_int * 1000000)

# import and prune user books
# TODO: change to custom user provided bookshelf
user = pd.read_csv(PATH+EXPORT,  dtype=str).fillna('')
dropped_books  = 0
for index,row in user.iterrows():
    if 'to-read' in row['Bookshelves'] or (row['Year Published'] != "" and int(row['Year Published']) > 2017): #  or row['My Rating'] == '0'
        user = user.drop(index)
        index -= 1
        dropped_books += 1
print("Removed", dropped_books, "unread, or too recent books, for a total of", user.shape[0], "books to check")
print("-------------")

# import book id to interactions id spreadsheet
id_map = pd.read_csv(PATH+'book_id_map.csv', dtype=str)

# convert user book id to book_id_csv (parallelized)
procs = mp.cpu_count() - 4
chunks = np.array_split(user, procs)
print("Checking books...")
with mp.Pool(processes=procs) as pool:
    result = pool.map(__lookup_books, chunks)
    similar_readers = {}
    user_csv_ids = []
    for output in result:
        user_csv_ids.extend(output[1])
        for key, value in output[0].items():
            similar_readers[key] = value if not(key in similar_readers.keys()) else value + similar_readers[key]
        

# sort dict by values in descending order
readers = dict(sorted(similar_readers.items(), key=lambda item: item[1], reverse=True))
closest = dict(list(readers.items())[0:10]) if len(list(readers.items())) > 10 else dict(list(readers.items())[0:])
print("-------------\nClosest users / num matching books")
for key, value in closest.items():
    print(f"{key}: {value}")
print("-------------")

# get all items from each closest (dump all book_ids into a new dict, get highest values again?)
close_books = {}
# get every single one of their books (c function)
for reader in closest:
    readers_books = fun.getListBooks(reader).contents
    print(f"Reader {reader} read {get_length(readers_books)} books.")
    for book in readers_books:
        if book == -1: # end of array
            break
        if not(book in user_csv_ids): # user hasn't read book yet
            if book in close_books:
                close_books[book] += 1
            else:
                close_books[book] = 1

# most common books in group not in user_csv_ids
# TODO: get all books recommended by all 10 and then randomize selection for different output each time?
books = dict(sorted(close_books.items(), key=lambda item: item[1], reverse=True))
# TODO: maybe get the 5 least popular books that all 10 users liked (or most of them did)
good_books = {}
agreeing_users = 10
i = 0
# get at least 5 of the most recommended books and all other books of that final recommended level
while len(good_books) < 5 and agreeing_users == list(books.values())[i]:
    good_books[list(books.keys())[i]] = fun.getNumUsers(list(books.keys())[i])
    i+=1
# then sort by popularity, getting the least well-known, so it doesn't just always recommend reading Harry Potter or Twilight
goodbooks = dict(sorted(good_books.items(), key=lambda item: item[1], reverse=False))
# Choose the 5 least popular books
best_books = dict(list(goodbooks.items())[0:5])

print("-------------")

print("Here are your book recommendations:")
# convert from book_id_csv to book_id
for book in best_books.keys():
    book_id_series = id_map.loc[(id_map['book_id_csv'] == str(book)), 'book_id']
    book_id = int(book_id_series.iloc[0])
    print(f"https://www.goodreads.com/book/show/{book_id}")

# small - 79 books, 855 seconds (14.25 min) -- 10.82 sec/book
# parallel- 236 sec or 3.9 min

# small2 - 166 books
# parallel - 356 sec or 5.9 min

# med - 582 books, took 5802 seconds (96.7 min or 1 hr 36.7 min) -- 9.97 sec/book
# parallel - 1152 seconds or 19.2 min

# large - 883 books, took -10008- 11091 seconds (166.8 min or 2hr 46.8min) -- 11.33 sec/book
# parallel - 1830 seconds or 30.5 min

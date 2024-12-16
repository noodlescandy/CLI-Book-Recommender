#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// longest line in goodreads_books.json is roughly 65,983
#define BUFFER_SIZE 30 

// compile with: cc -fPIC -shared -o libbook.so find_all.c
// gets list of users with that book_id
// used in first part for finding the users who have that book

int *getListUsers(int book_id) {
    FILE* file = fopen("../data/goodreads_interactions.csv", "r");
    char line[BUFFER_SIZE];
    if (file == NULL){
        fprintf(stderr, "Unable to locate file!\n");
        exit(1);
    }

    int* userIDs = (int*)malloc(877000 * sizeof(int)); // 876145 total users
    if (userIDs == NULL){
        fprintf(stderr, "Memory Allocation Failed.\n");
        exit(1);
    }

    int i = 0;
    char id[10]; // 10 is max int digits
    sprintf(id, "%d", book_id);
    while(fgets(line, sizeof(line), file)) {
        char* tok = strtok(line, ","); // user_id
        char* token = strtok(NULL, ","); // book_id
        if (strcmp(token, id) == 0) { // book_id = wanted id
            int user_id = atoi(tok); // first token to int
            userIDs[i] = user_id;
            i++;
        }
    }

    userIDs[i] = -1; // end of array
    fclose(file);
    return userIDs;
}

// gets list of books with that user_id
// used in second part for finding a user's books
int *getListBooks(int user_id) {
    FILE* file = fopen("../data/goodreads_interactions.csv", "r");
    char line[BUFFER_SIZE];
    if (file == NULL){
        fprintf(stderr, "Unable to locate file!\n");
        exit(1);
    }

    int* bookIDs = (int*)malloc(1000000 * sizeof(int)); 
    // unlikely anyone has over 1 million read books, but there are numerous over 100k. Would see a crash in main/seg fault
    if (bookIDs == NULL){
        fprintf(stderr, "Memory Allocation Failed.\n");
        exit(1);
    }

    int i = 0;
    char id[11]; // 10 is max int digits + 2 for pre/suf-fix
    sprintf(id, "%d", user_id);
    int flag = 0;
    while(fgets(line, sizeof(line), file)) {
        char* tok = strtok(line, ","); // search only first token for better matching
        if (strcmp(tok, id) == 0) { // found it
            char* token = strtok(NULL, ","); // get second?
            int book_id = atoi(token); // second token to int
            bookIDs[i] = book_id;
            i++;
            flag = 1;
        }
        else if (flag){ 
            // in the file, all of one user is sequential, meaning when they're done, it's done.
            break;
        }
    }
    bookIDs[i] = -1; // end of array
    fclose(file);
    return bookIDs;
}

// gets num users with that book_id (also shows popularity)
int getNumUsers(int book_id) {
    FILE* file = fopen("../data/goodreads_interactions.csv", "r");
    char line[BUFFER_SIZE];
    if (file == NULL){
        fprintf(stderr, "Unable to locate file!\n");
        return -2;
    }
    int c = 0;
    char id[12]; // 10 is max int digits + 2 for pre/suf-fix
    
    if (book_id > 5){
        sprintf(id, ",%d,", book_id);
        while(fgets(line, sizeof(line), file)) {
            if (strstr(line, id) != NULL) { // found it
                c++;
            }
        }
    }
    else{ // slower version, but doesn't get caught on ratings
        sprintf(id, "%d", book_id);
        while(fgets(line, sizeof(line), file)) {
            char* tok = strtok(line, ","); // user_id
            char* token = strtok(NULL, ","); // book_id
            if (strcmp(token, id) == 0) { // book_id = wanted id
                c++;
            }
        }
    }
    fclose(file);
    return c;
}

/*
// small testing function for debugging problem ids
int main(){
    printf("Getting id..\n");
    int book_id = 5;
    getListUsers(book_id);
    return 0;
}
*/

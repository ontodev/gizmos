
inputData=$1  #currently use OBI

database="exampleNoStanza.db" #NB: this database name is hardcoded in (***)
stanzaDatabase="exampleStanza.db"

#1. create database & #create table
sqlite3 $database "create table statements(subject,predicate,object);"

#2. import data (use tab mode!)
sqlite3 $database -cmd ".mode tab" ".import $inputData statements"

#3. generate stanzas with recursive query (includes hardcoded database)
sqlite3 $stanzaDatabase < generateStanza.sql #(***)

#3. ALTERNATIVE in which only root stanzas, i.e., non-blank nodes
#sqlite3 $stanzaDatabase < generateRootStanza.sql #


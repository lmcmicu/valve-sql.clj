MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := example.db
.DELETE_ON_ERROR:
.SUFFIXES:

example.db: create_example_db.sql
	sqlite3 example.db < create_example_db.sql

.PHONY: clean

clean:
	rm -f example.db

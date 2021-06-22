MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := round-trip
.DELETE_ON_ERROR:
.SUFFIXES:

#args = --log --filter 'obo:CLO_0000001'
#args = --log --filter 'BFO:0000027'
#args = --log --filter 'OBI:0100061'

# round-trip: build/obi_core.tsv obi_core_no_trailing_ws.owl
round-trip: tests/thin.tsv tests/resources/example.rdf
	tests/prototype.py $(args) $^

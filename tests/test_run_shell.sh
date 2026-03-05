#!/bin/bash

# Function to print to stdout
print_to_stdout() {
    echo "This is a message to stdout"
}

# Function to print to stderr
print_to_stderr() {
    echo "This is an error message to stderr" 1>&2
}

# Function to print to another console (e.g., /dev/tty1)
print_to_other_console() {
    echo "This is a message to another console" > /dev/tty1
}

# Invoke the functions
print_to_stdout
print_to_stderr

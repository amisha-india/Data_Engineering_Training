import string_utils
sentence = "Hii!!!  I am Amisha , Nice to meet you...."

reversed_sentence = string_utils.reverse_string(sentence)
capitalized_sentence = string_utils.capitalize_words(sentence)
vowel_count = string_utils.count_vowels(sentence)
is_sentence_palindrome = string_utils.is_palindrome(sentence)

print(f"Original Sentence: {sentence}")
print(f"Reversed Sentence: {reversed_sentence}")
print(f"Capitalized Sentence: {capitalized_sentence}")
print(f"Number of Vowels: {vowel_count}")
print(f"Is Palindrome: {is_sentence_palindrome}")
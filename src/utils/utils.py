import re 

def clean_text(string, repl=' '):
    ss = re.sub(r'[^a-z0-9]', repl, string.lower())
    return ss
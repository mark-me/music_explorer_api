list_value_part = [
    "piano", "vocal", "perform", "bass", "viol", "drum", "keyboard", "guitar", "sax",
    "music", "written", "arrange", "lyric", "word", "compose", "song", "accordion",
    "chamberlin", "clarinet", "banjo", "band", "bongo", "bell", "bouzouki", "brass",
    "cello", "cavaquinho", "celest", "choir", "chorus", "handclap", "conduct", "conga",
    "percussion", "trumpet", "cornet", "djembe", "dobro", "organ", "electron", "horn",
    "fiddle", "flute", "recorder", "glocken", "gong", "guest", "vibra", "harmonium",
    "harmonica", "harp", "beatbox", "leader", "loop", "MC", "mellotron", "melod",
    "mixed", "oboe", "orchestra", "recorded", "remix", "saw", "score", "sitar",
    "strings", "synth", "tabla", "tambourine", "theremin", "timbales", "timpani",
    "whistle", "triangle", "trombone", "tuba", "vocoder", "voice", "phone", "woodwind",
]
last_value = list_value_part.pop(-1)
sql_start = "UPDATE role SET as_edge = 1 WHERE "
sql_statement = (
    sql_start
    + "".join(f"""role LIKE "%{value_part}%" OR """ for value_part in list_value_part)
    + f"""role LIKE "%{last_value}%" """
)

print(sql_statement)
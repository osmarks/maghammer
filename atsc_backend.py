import whisperx
import sys
import time
import sqlite3
import psycopg2

device = "cuda" 
batch_size = 16
compute_type = "float16"

model = whisperx.load_model("large-v2", device, compute_type=compute_type, language="en")
model_a, metadata = whisperx.load_align_model(language_code="en", device=device)

print("Models loaded.")

BASE = "/media/"

conn = psycopg2.connect("dbname=maghammer user=maghammer")
conn2 = psycopg2.connect("dbname=maghammer user=maghammer")
csr = conn.cursor()
csr2 = conn.cursor()
csr.execute("SELECT id, path FROM media_files WHERE auto_subs_state = 1") # PENDING

def format_duration(seconds):
    hours = int(seconds / 3600.0)
    seconds -= 3600.0 * hours
    minutes = int(seconds / 60.0)
    seconds -= 60.0 * minutes
    full_seconds = int(seconds)
    return f"{hours:02}:{minutes:02}:{full_seconds:02}"

while row := csr.fetchone():
    file = row[1]
    docid = row[0]
    start = time.time()
    skip = False
    subs = ""
    try:
        audio = whisperx.load_audio(BASE + file)
    except Exception as e:
        print(e)
        skip = True
    
    if not skip:
        loaded = time.time()
        result = model.transcribe(audio, batch_size=batch_size)
        transcribed = time.time()

        result = whisperx.align(result["segments"], model_a, metadata, audio, device, return_char_alignments=False)
        aligned = time.time()

        print(f"{file} x{len(result["segments"])} load={loaded-start:1f}s transcribe={transcribed - loaded:1f}s align={aligned - transcribed:1f}s")

        for seg in result["segments"]:
            subs += f"[{format_duration(seg['start'])} -> {format_duration(seg['end'])}]: {seg['text'].strip()}\n"
        subs = subs.strip()

    csr2.execute("UPDATE media_files SET subs = %s, auto_subs_state = 2 WHERE id = %s", (subs, docid)) # GENERATED
    conn2.commit()
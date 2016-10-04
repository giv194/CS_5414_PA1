Slip days used (this project): __2___ Slip days used (total): __2(pla36)__3(kr522)__
Kirill Rudenko (kr522) and Paul Ammann (pla36)

This program creates a distributed store of key value pairs. It is simply run via:
python master.py

one can initialize a predetermined set of n nodes via
<id> start <n> <port>

Where port is the port master communicates with the process.
you can query any process with
<id> get <key>
and it will return value, which master will print.

additionally, you can send commands to the coordinator via:
-1 add <key> <value>
-1 delete <key> <value>

To exit master, run
exit
This will wipe the dt-logs for all participants.

There are additional commands to test the integrity of this protocol under various conditions:
⟨id⟩ crash
crash
the receiver process crashes itself immediately

⟨id⟩ vote NO
vote NO
the receiver vote NO to the next add/delete command

⟨id⟩ crashAfterVote
crashAfterVote
the receiver process crashes itself after sending vote response
if it is participant

⟨id⟩ crashAfterAck
crashAfterAck
the receiver process crashes itself after sending ack response
if it is participant

⟨id⟩ crashVoteREQ 2 3
crashVoteREQ 2 3
the receiver process crashes itself after sending vote request to processes 2 3 if it is coordinator

⟨id⟩ crashPartialPreCommit 2 3
crashPartialPreCommit 2 3
the receiver process crashes itself after sending precommit to processes 2 3 if it is coordinator

⟨id⟩ crashPartialCommit 2 3
crashPartialCommit 2 3
the receiver process crashes itself after sending commit to processes 2 3 if it is coordinator

----------------------------
TESTING
____________________________
There is a file called grading.py which will run simple test cases in tests folder where you specify input cases in .input and expected output in .output.

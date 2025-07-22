use FitnessCenter
switched to db FitnessCenter
db["members"].find()

// 1.members

db.members.insertMany([
{ member_id: 1, name: "Anjali Rao", age: 28, gender: "Female", city: "Mumbai",
membership_type: "Gold" },
{ member_id: 2, name: "Rohan Mehta", age: 35, gender: "Male", city: "Delhi",
membership_type: "Silver" },
{ member_id: 3, name: "Fatima Shaikh", age: 22, gender: "Female", city: "Hyderabad",
membership_type: "Platinum" },
{ member_id: 4, name: "Vikram Das", age: 41, gender: "Male", city: "Bangalore",
membership_type: "Gold" },
{ member_id: 5, name: "Neha Kapoor", age: 31, gender: "Female", city: "Pune",
membership_type: "Silver" }
])
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f1e2d6cb8e855922c9ff0'),
    '1': ObjectId('687f1e2d6cb8e855922c9ff1'),
    '2': ObjectId('687f1e2d6cb8e855922c9ff2'),
    '3': ObjectId('687f1e2d6cb8e855922c9ff3'),
    '4': ObjectId('687f1e2d6cb8e855922c9ff4')
  }
}

// 2.trainers

db.trainers.insertMany([
{ trainer_id: 101, name: "Ajay Kumar", specialty: "Weight Training", experience: 7
},
{ trainer_id: 102, name: "Swati Nair", specialty: "Cardio", experience: 5 },
{ trainer_id: 103, name: "Imran Qureshi", specialty: "Yoga", experience: 8 }
])
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f1e816cb8e855922c9ff5'),
    '1': ObjectId('687f1e816cb8e855922c9ff6'),
    '2': ObjectId('687f1e816cb8e855922c9ff7')
  }
}

// 3.sessions

db.sessions.insertMany([
{ session_id: 201, member_id: 1, trainer_id: 101, session_type: "Strength",
duration: 60, date: new Date("2024-08-01") },
{ session_id: 202, member_id: 2, trainer_id: 102, session_type: "Cardio", duration:
45, date: new Date("2024-08-02") },
{ session_id: 203, member_id: 3, trainer_id: 103, session_type: "Yoga", duration:
90, date: new Date("2024-08-03") },
{ session_id: 204, member_id: 1, trainer_id: 102, session_type: "Cardio", duration:
30, date: new Date("2024-08-04") },
{ session_id: 205, member_id: 4, trainer_id: 101, session_type: "Strength",
duration: 75, date: new Date("2024-08-05") },
{ session_id: 206, member_id: 5, trainer_id: 103, session_type: "Yoga", duration:
60, date: new Date("2024-08-05") }
])
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f1ecb6cb8e855922c9ff8'),
    '1': ObjectId('687f1ecb6cb8e855922c9ff9'),
    '2': ObjectId('687f1ecb6cb8e855922c9ffa'),
    '3': ObjectId('687f1ecb6cb8e855922c9ffb'),
    '4': ObjectId('687f1ecb6cb8e855922c9ffc'),
    '5': ObjectId('687f1ecb6cb8e855922c9ffd')
  }
}

// Basic Queries

// 1. Find all members from Mumbai.
db.members.find({ city: "Mumbai" })

// 2. List all trainers with experience greater than 6 years.
db.trainers.find({ experience: { $gt: 6 } })

// 3. Get all Yoga sessions.
db.sessions.find({ session_type: "Yoga" })

// 4. Show all sessions conducted by trainer Swati Nair.
db.sessions.find({trainer_id: db.trainers.findOne({name: "Swati Nair"}).trainer_id})

// 5. Find all members who attended a session on 2024-08-05.
db.sessions.find({date: new Date("2024-08-05")})

// Intermediate Queries

// 6. Count the number of sessions each member has attended.
db.sessions.aggregate([{$group: {_id: "$member_id", sessionCount: {$sum: 1}}}])

// 7. Show average duration of sessions for each session_type.
db.sessions.aggregate([{$group: {_id: "$session_type", avgDuration: {$avg: "$duration"}}}])

// 8. Find all female members who attended a session longer than 60 minutes.
db.sessions.aggregate([{$match: {duration: { $gt: 60 } } },{$lookup: {from: "members", localField: "member_id", foreignField: "member_id", as: "member_info"}},{$unwind: "$member_info"},{$match: {"member_info.gender": "Female"}},{$project: {_id: 0, name: "$member_info.name", duration: 1}}])

// 9. Display sessions sorted by duration (descending).
db.sessions.find().sort({ duration: -1 })

// 10. Find members who have attended sessions with more than one trainer.
db.sessions.aggregate([{$group: {_id: "$member_id", uniqueTrainers: {$addToSet: "$trainer_id"}}},{$project: {trainerCount: {$size: "$uniqueTrainers"}}},{$match: {trainerCount: {$gt: 1}}}])

// Advanced Queries with Aggregation & Lookup

// 11. Use $lookup to display sessions with member name and trainer name.
db.sessions.aggregate([{$lookup: {from: "members",localField: "member_id",foreignField: "member_id",as: "member"}},{$lookup: {from: "trainers",localField: "trainer_id",foreignField: "trainer_id",as: "trainer"}},{$unwind: "$member"},{$unwind: "$trainer"},{$project: {session_id: 1,session_type: 1,duration: 1,"member.name": 1,"trainer.name": 1}}])

// 12. Calculate total session time per trainer.
db.sessions.aggregate([{$group: {_id: "$trainer_id", totalDuration: {$sum: "$duration"}}}])

// 13. List each member and their total time spent in the gym.
db.sessions.aggregate([{$group: {_id: "$member_id", totalDuration: {$sum: "$duration"}}}])

// 14. Count how many sessions each trainer has conducted.
db.sessions.aggregate([{$group: {_id: "$trainer_id", sessionCount: {$sum: 1}}}])

// 15. Find which trainer has conducted the longest average session duration.
db.sessions.aggregate([{$group: {_id: "$trainer_id", avgDuration: {$avg: "$duration"}}},{$sort: {avgDuration: -1}},{$limit: 1}])

// 16. Show how many unique members each trainer has trained.
db.sessions.aggregate([{$group: {_id: "$trainer_id", uniqueMembers: {$addToSet: "$member_id"}}},{$project: {memberCount: {$size: "$uniqueMembers"}}}])

// 17. Find the most active member (by total session duration).
db.sessions.aggregate([{$group: {_id: "$member_id", totalDuration: {$sum: "$duration"}}},{$sort: {totalDuration: -1}},{$limit: 1}])

// 18. List all Gold membership members who took at least one Strength session.
db.sessions.aggregate([{$match: {session_type: "Strength"}},{$lookup: {from: "members",localField: "member_id",foreignField: "member_id",as: "member"}},{$unwind: "$member"},{$match: {"member.membership_type": "Gold"}},{$project: {_id: 0, "member.name": 1}},{$group: {_id: "$member.name"}}])

// 19. Display a breakdown of sessions by membership type.
db.sessions.aggregate([{$lookup: {from: "members",localField: "member_id",foreignField: "member_id",as: "member"}},{$unwind: "$member"},{$group: {_id: "$member.membership_type", sessionCount: {$sum: 1}}}])

// 20. Find members who have not attended any session yet (hint: simulate later by adding a new member).
db.members.insertOne({ member_id: 6, name: "Kiran Sharma", age: 29, gender: "Male", city: "Chennai", membership_type: "Platinum" })

db.members.aggregate([{$lookup: {from: "sessions",localField: "member_id",foreignField: "member_id",as: "sessions"}},{$match: {sessions: {$eq: []}}}])

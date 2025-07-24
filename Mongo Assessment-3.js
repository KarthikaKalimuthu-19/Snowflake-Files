// MongoDB Assignment: Design & Query Challenge

use JobPortal;
switched to db JobPortal

// Part 1: Create Collections and Insert Your Own Data

db.jobs.insertMany([
  { job_id: 1, title: "Backend Developer", company: "TechNova", location: "Bangalore", salary: 1200000, job_type: "remote", posted_on: new Date("2024-07-01") },
  { job_id: 2, title: "Data Analyst", company: "InnoData", location: "Delhi", salary: 900000, job_type: "hybrid", posted_on: new Date("2024-06-15") },
  { job_id: 3, title: "Frontend Developer", company: "PixelWorks", location: "Remote", salary: 1100000, job_type: "remote", posted_on: new Date("2024-07-10") },
  { job_id: 4, title: "DevOps Engineer", company: "CloudSync", location: "Hyderabad", salary: 1300000, job_type: "on-site", posted_on: new Date("2024-05-20") },
  { job_id: 5, title: "UI/UX Designer", company: "DesignHub", location: "Pune", salary: 800000, job_type: "hybrid", posted_on: new Date("2024-07-05") }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('68820494de1c0638278ccfc8'),
    '1': ObjectId('68820494de1c0638278ccfc9'),
    '2': ObjectId('68820494de1c0638278ccfca'),
    '3': ObjectId('68820494de1c0638278ccfcb'),
    '4': ObjectId('68820494de1c0638278ccfcc')
  }
}

db.applicants.insertMany([
  { applicant_id: 101, name: "Ananya Rao", skills: ["MongoDB", "React", "Node.js"], experience: 2, city: "Hyderabad", applied_on: new Date("2024-07-12") },
  { applicant_id: 102, name: "Vikram Singh", skills: ["Python", "SQL"], experience: 3, city: "Delhi", applied_on: new Date("2024-07-14") },
  { applicant_id: 103, name: "Sara Iqbal", skills: ["MongoDB", "Python", "Flask"], experience: 1, city: "Bangalore", applied_on: new Date("2024-07-10") },
  { applicant_id: 104, name: "Ramesh Patel", skills: ["Java", "Spring"], experience: 4, city: "Hyderabad", applied_on: new Date("2024-07-11") },
  { applicant_id: 105, name: "Priya Nair", skills: ["HTML", "CSS", "JavaScript"], experience: 2, city: "Mumbai", applied_on: new Date("2024-07-08") }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('688204b6de1c0638278ccfcd'),
    '1': ObjectId('688204b6de1c0638278ccfce'),
    '2': ObjectId('688204b6de1c0638278ccfcf'),
    '3': ObjectId('688204b6de1c0638278ccfd0'),
    '4': ObjectId('688204b6de1c0638278ccfd1')
  }
}

db.applications.insertMany([
  { application_id: 201, applicant_id: 101, job_id: 1, application_status: "interview scheduled", interview_scheduled: true, feedback: "Good" },
  { application_id: 202, applicant_id: 102, job_id: 2, application_status: "applied", interview_scheduled: false, feedback: "Pending" },
  { application_id: 203, applicant_id: 103, job_id: 1, application_status: "interview scheduled", interview_scheduled: true, feedback: "Excellent" },
  { application_id: 204, applicant_id: 103, job_id: 3, application_status: "applied", interview_scheduled: false, feedback: "Pending" },
  { application_id: 205, applicant_id: 105, job_id: 5, application_status: "rejected", interview_scheduled: false, feedback: "Average" }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('688204e2de1c0638278ccfd2'),
    '1': ObjectId('688204e2de1c0638278ccfd3'),
    '2': ObjectId('688204e2de1c0638278ccfd4'),
    '3': ObjectId('688204e2de1c0638278ccfd5'),
    '4': ObjectId('688204e2de1c0638278ccfd6')
  }
}

// Part 2: Write the Following Queries
// 1. Find all remote jobs with a salary greater than 10,00,000.
db.jobs.find({job_type: "remote", salary: {$gt: 1000000}})

// 2. Get all applicants who know MongoDB.
db.applicants.find({skills: "MongoDB"})

// 3. Show the number of jobs posted in the last 30 days.
db.jobs.find({posted_on: {$gte: new Date(new Date() - 1000 * 60 * 60 * 24 * 30)}}).count()

// 4. List all job applications that are in ‘interview scheduled’ status.
db.applications.find({application_status: "interview scheduled"})

// 5. Find companies that have posted more than 2 jobs.****
db.jobs.aggregate([
  {$group: {_id: "$company", job_count: {$sum: 1}}},
  {$match: {job_count: {$gt: 2}}}
])

// Part 3: Use $lookup and Aggregation
// 6. Join applications with jobs to show job title along with the applicant’s name.
db.applications.aggregate([
  {
    $lookup: {
      from: "jobs",
      localField: "job_id",
      foreignField: "job_id",
      as: "job_info"
    }
  },
  {$unwind: "$job_info"},
  {
    $lookup: {
      from: "applicants",
      localField: "applicant_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  {$unwind: "$applicant_info"},
  {
    $project: {
      job_title: "$job_info.title",
      applicant_name: "$applicant_info.name"
    }
  }
])

// 7. Find how many applications each job has received.
db.applications.aggregate([
  {$group: {_id: "$job_id", tot_applications: {$sum: 1}}}
])

// 8. List applicants who have applied for more than one job.
db.applications.aggregate([
  {$group: {_id: "$applicant_id", count: {$sum: 1}}},
  {$match: {count: {$gt: 1}}},
  {
    $lookup: {
      from: "applicants",
      localField: "_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  {$unwind: "$applicant_info"},
  {
    $project: {
      name: "$applicant_info.name",
      applications: "$count"
    }
  }
])

// 9. Show the top 3 cities with the most applicants.
db.applicants.aggregate([
  {$group: {_id: "$city", applicant_count: {$sum: 1}}},
  {$sort: {applicant_count: -1}},
  {$limit: 3}
])

// 10. Get the average salary for each job type (remote, hybrid, on-site).
db.jobs.aggregate([
  {$group: {_id: "$job_type", avg_salary: {$avg: "$salary"}}}
])

// Part 4: Data Updates
// 11. Update the status of one application to "offer made".
db.applications.updateOne(
  {application_id: 201},
  {$set: {application_status: "offer made"}}
)
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

// 12. Delete a job that has not received any applications.
const usedJobIds = db.applications.distinct("job_id")

db.jobs.deleteMany({job_id: {$nin: usedJobIds}})
{
  acknowledged: true,
  deletedCount: 1
}

// 13. Add a new field shortlisted to all applications and set it to false.
db.applications.updateMany({}, {$set: {shortlisted: false}})
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 5,
  modifiedCount: 5,
  upsertedCount: 0
}

// 14. Increment experience of all applicants from "Hyderabad" by 1 year.
db.applicants.updateMany(
  {city: "Hyderabad"},
  {$inc: {experience: 1}}
)
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 2,
  modifiedCount: 2,
  upsertedCount: 0
}

// 15. Remove all applicants who haven’t applied to any job.
const activeIds = db.applications.distinct("applicant_id")

db.applicants.deleteMany({applicant_id: {$nin: activeIds}})
{
  acknowledged: true,
  deletedCount: 1
}


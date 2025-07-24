// MongoDB Assignment: Library Management System

use Library_Management_System;
switched to db Library_Management_System

// Step 1: Create Collections and Insert Sample Data

// 1. books

db.books.insertMany([
{ book_id: 201, title: "The Alchemist", author: "Paulo Coelho", genre: "Fiction",
copies: 10 },
{ book_id: 202, title: "Atomic Habits", author: "James Clear", genre: "Self-Help",
copies: 5 },
{ book_id: 203, title: "Sapiens", author: "Yuval Noah Harari", genre: "History",
copies: 7 },
{ book_id: 204, title: "The Lean Startup", author: "Eric Ries", genre: "Business",
copies: 3 },
{ book_id: 205, title: "Deep Work", author: "Cal Newport", genre: "Productivity",
copies: 4 }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('6881ec313815ec44970466cd'),
    '1': ObjectId('6881ec313815ec44970466ce'),
    '2': ObjectId('6881ec313815ec44970466cf'),
    '3': ObjectId('6881ec313815ec44970466d0'),
    '4': ObjectId('6881ec313815ec44970466d1')
  }
}

// 2. members

db.members.insertMany([
{ member_id: 101, name: "Ayesha Khan", joined_on: new Date("2024-01-15") },
{ member_id: 102, name: "Rahul Verma", joined_on: new Date("2024-03-12") },
{ member_id: 103, name: "Nikita Rao", joined_on: new Date("2024-04-10") }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('6881ec823815ec44970466d2'),
    '1': ObjectId('6881ec823815ec44970466d3'),
    '2': ObjectId('6881ec823815ec44970466d4')
  }
}

// 3. borrowed

db.borrowed.insertMany([
{ borrow_id: 1, member_id: 101, book_id: 201, date: new Date("2024-06-01"),
returned: true },
{ borrow_id: 2, member_id: 101, book_id: 203, date: new Date("2024-06-15"),
returned: false },
{ borrow_id: 3, member_id: 102, book_id: 202, date: new Date("2024-06-20"),
returned: false },
{ borrow_id: 4, member_id: 103, book_id: 204, date: new Date("2024-06-22"),
returned: true }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('6881ecb23815ec44970466d5'),
    '1': ObjectId('6881ecb23815ec44970466d6'),
    '2': ObjectId('6881ecb23815ec44970466d7'),
    '3': ObjectId('6881ecb23815ec44970466d8')
  }
}

// Step 2: Query Exercises

// Basic Queries
// 1. Find all books in the Self-Help genre.
db.books.find({genre: "Self-Help"})

// 2. Show members who joined after March 2024.
db.members.find({joined_on: {$gt: new Date("2024-03-31")}})

// 3. List all borrowed books that have not been returned.
db.borrowed.find({returned: false})

// 4. Display all books with fewer than 5 copies in stock.
db.books.find({copies: {$lt: 5}})

// 5. Get details of all books written by Cal Newport.
db.books.find({author: "Cal Newport"})

// Join Queries using $lookup
// 6. List all borrow records with book title and member name.
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  {$unwind: "$member_info"},
  {
    $project: {
      borrow_id: 1,
      date: 1,
      returned: 1,
      member_name: "$member_info.name",
      book_title: "$book_info.title"
    }
  }
])

// 7. Find which member borrowed "Sapiens".
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {$match: {"book_info.title": "Sapiens"}},
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  {$unwind: "$member_info"},
  {
    $project: {
      member_name: "$member_info.name"
    }
  }
])

// 8. Display all members along with the books they've borrowed.
db.members.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "member_id",
      foreignField: "member_id",
      as: "borrowed_books"
    }
  }
])

// 9. Get a list of members who have borrowed books and not returned them.
db.borrowed.aggregate([
  {$match: {returned: false}},
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  {$unwind: "$member_info"},
  {
    $group: {
      _id: "$member_info.name"
    }
  }
])

// 10. Show each book along with how many times it has been borrowed.
db.borrowed.aggregate([
  {
    $group: {
      _id: "$book_id",
      times_borrowed: {$sum: 1}
    }
  },
  {
    $lookup: {
      from: "books",
      localField: "_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {
    $project: {
      title: "$book_info.title",
      times_borrowed: 1
    }
  }
])

// Aggregation & Analysis
// 11. Count how many books each member has borrowed.
db.borrowed.aggregate([
  {
    $group: {
      _id: "$member_id",
      tot_borrowed: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "members",
      localField: "_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$member_info" },
  {
    $project: {
      member_name: "$member_info.name",
      tot_borrowed: 1
    }
  }
])

// 12. Which genre has the highest number of books?
db.books.aggregate([
  {
    $group: {
      _id: "$genre",
      total_books: {$sum: "$copies"}
    }
  },
  {$sort: {total_books: -1}},
  {$limit: 1}
])

// 13. List the top 2 most borrowed books.
db.borrowed.aggregate([
  {
    $group: {
      _id: "$book_id",
      borrow_cnt: {$sum: 1}
    }
  },
  {$sort: {borrow_cnt: -1}},
  {$limit: 2},
  {
    $lookup: {
      from: "books",
      localField: "_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {
    $project: {
      title: "$book_info.title",
      borrow_cnt: 1
    }
  }
])

// 14. Show the average number of copies available per genre.
db.books.aggregate([
  {
    $group: {
      _id: "$genre",
      avg_copies: {$avg: "$copies"}
    }
  }
])

// 15. Find the total number of books currently borrowed (not returned).
db.borrowed.aggregate([
  {$match: {returned: false}},
  {
    $group: {
      _id: null,
      tot_currently_borrowed: {$sum: 1}
    }
  }
])

// Advanced Scenarios
// 16. Add a new member who hasn't borrowed any book. Write a query to list such members.
db.members.insertOne({member_id: 104, name: "Kabir Singh", joined_on: new Date("2024-07-24")})
{
  acknowledged: true,
  insertedId: ObjectId('6881f0963815ec44970466d9')
}

db.members.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "member_id",
      foreignField: "member_id",
      as: "borrow_info"
    }
  },
  {$match: {borrow_info: {$eq: []}}}
])

// 17. Identify books that have never been borrowed.
db.books.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "book_id",
      foreignField: "book_id",
      as: "borrow_info"
    }
  },
  {$match: {borrow_info: {$eq: []}}}
])

// 18. Get the name of members who borrowed more than one book.
db.borrowed.aggregate([
  {
    $group: {
      _id: "$member_id",
      borrow_cnt: {$sum: 1}
    }
  },
  {$match: {borrow_cnt: {$gt: 1}}},
  {
    $lookup: {
      from: "members",
      localField: "_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  {$unwind: "$member_info"},
  {
    $project: {
      member_name: "$member_info.name",
      borrow_cnt: 1
    }
  }
])

// 19. Display borrowing trends by month (group by date ).
db.borrowed.aggregate([
  {
    $group: {
      _id: {$month: "$date"},
      borrow_cnt: {$sum: 1}
    }
  },
  {$sort: {"_id": 1}}
])

// 20. Show borrow records where the borrowed book had fewer than 5 copies at the time of borrowing.
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {$match: {"book_info.copies": {$lt: 5}}},
  {
    $project: {
      borrow_id: 1,
      book_title: "$book_info.title",
      copies: "$book_info.copies"
    }
  }
])

// Bonus Questions
// Simulate overdue books by adding a due_date and finding overdue records.
db.borrowed.updateOne(
  {borrow_id: 2},
  {$set: {due_date: new Date("2024-06-25")}}
)
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

db.borrowed.find({
  returned: false,
  due_date: {$lt: new Date("2024-07-24")}
})

// Create a chart-style output showing how many books are borrowed per genre.
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {$unwind: "$book_info"},
  {
    $group: {
      _id: "$book_info.genre",
      borrowed_cnt: {$sum: 1}
    }
  },
  {$sort: {borrowed_cnt: -1}}
])

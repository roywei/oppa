// Fixtures the Mac connector cannot make itself, for oppa-verify's mac-connector drive:
// a calendar on this Mac only, a reminders list, removal of either with everything in it,
// and the ids of every calendar and list with a title, one a line. Prints the id or ids, or
// an error and exits 1.
//   calendar <title> | list <title> | remove <calendar or list id> | find <title>
import EventKit
import Foundation

let store = EKEventStore()
let args = CommandLine.arguments

func fail(_ message: String) -> Never {
    print("error: \(message)")
    exit(1)
}

switch (args.count > 1 ? args[1] : "", args.count) {
case ("calendar", 3):
    guard let local = store.sources.first(where: { $0.sourceType == .local }) else {
        fail("this Mac has no On My Mac calendars")
    }
    let calendar = EKCalendar(for: .event, eventStore: store)
    calendar.title = args[2]
    calendar.source = local
    do { try store.saveCalendar(calendar, commit: true) } catch { fail("\(error.localizedDescription)") }
    print(calendar.calendarIdentifier)
case ("list", 3):
    guard let source = store.defaultCalendarForNewReminders()?.source else { fail("this Mac has no reminders list") }
    let list = EKCalendar(for: .reminder, eventStore: store)
    list.title = args[2]
    list.source = source
    do { try store.saveCalendar(list, commit: true) } catch { fail("\(error.localizedDescription)") }
    print(list.calendarIdentifier)
case ("find", 3):
    // Without full access EventKit lists nothing, which would look like nothing left.
    guard [EKEntityType.event, .reminder].allSatisfy({ EKEventStore.authorizationStatus(for: $0) == .fullAccess }) else {
        fail("Calendars and Reminders access is needed to look")
    }
    for calendar in store.calendars(for: .event) + store.calendars(for: .reminder) where calendar.title == args[2] {
        print(calendar.calendarIdentifier)
    }
case ("remove", 3):
    guard let calendar = store.calendar(withIdentifier: args[2]) else { fail("no calendar has that id") }
    do { try store.removeCalendar(calendar, commit: true) } catch { fail("\(error.localizedDescription)") }
    print(args[2])
default:
    fail("usage: calendar <title> | list <title> | remove <calendar or list id> | find <title>")
}

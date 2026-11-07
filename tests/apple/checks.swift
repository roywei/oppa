// The Mac connector's due dates and repeats, applied to items that are never saved, so no
// Calendar or Reminders access is needed. tests/connectors.rs builds this with apple.swift,
// as main.swift, and -D TESTING, and reads the JSON it prints.
import EventKit
import Foundation

func shown(_ r: EKReminder) -> [String: Any] {
    var item: [String: Any] = ["alerts": (r.alarms ?? []).compactMap { $0.absoluteDate.map { iso.string(from: $0) } }]
    if let parts = r.dueDateComponents {
        item["due"] = [parts.hour, parts.minute, parts.second].map { $0.map(String.init) ?? "-" }.joined(separator: ":")
        if let when = Calendar.current.date(from: parts) { item["due_at"] = iso.string(from: when) }
    }
    return item
}

func runTests() {
    var dues: [String: Any] = [:]
    let requests: [(String, [String: Any])] = [("time", ["due": "2031-01-06T09:00:45"]), ("day", ["due": "2031-01-07"]),
                                              ("cleared", ["due": ""]), ("absent", [:]),
                                              // The first and the second 1:30 of the night clocks go back.
                                              ("daylight", ["due": "2030-11-03T01:30:00-07:00"]),
                                              ("standard", ["due": "2030-11-03T01:30:45-08:00"])]
    for (name, args) in requests {
        // Each starts with an earlier due time and its alert.
        let r = EKReminder(eventStore: store)
        r.dueDateComponents = DateComponents(year: 2030, month: 1, day: 1, hour: 8, minute: 0)
        r.addAlarm(EKAlarm(absoluteDate: Calendar.current.date(from: r.dueDateComponents!)!))
        due(args)?.apply(to: r)
        dues[name] = shown(r)
    }
    var repeats: [String: Any] = [:]
    let rules: [(String, [String: Any])] = [
        ("weekly", ["every": "week", "on": ["tuesday", "thursday"], "until": "2031-03-01"]),
        ("bimonthly", ["every": "month", "interval": 2, "until": "2031-06-01T09:00"]),
        ("biennial", ["every": "year", "interval": 2]),
        ("never", ["every": "never"])]
    for (name, given) in rules {
        // Each replaces an earlier daily rule.
        let e = EKEvent(eventStore: store)
        e.recurrenceRules = [EKRecurrenceRule(recurrenceWith: .daily, interval: 1, end: nil)]
        if let rule = repetition(["repeat": given]) { e.recurrenceRules = rule.map { [$0] } }
        repeats[name] = repeatItem(e) ?? NSNull()
    }
    // A rule made elsewhere, the last Friday of each month five times, reads back whole.
    let e = EKEvent(eventStore: store)
    e.recurrenceRules = [EKRecurrenceRule(recurrenceWith: .monthly, interval: 1,
        daysOfTheWeek: [EKRecurrenceDayOfWeek(.friday, weekNumber: -1)], daysOfTheMonth: nil, monthsOfTheYear: nil,
        weeksOfTheYear: nil, daysOfTheYear: nil, setPositions: nil, end: EKRecurrenceEnd(occurrenceCount: 5))]
    repeats["elsewhere"] = repeatItem(e) ?? NSNull()
    // A rule as an import can bring it, with every field only Calendar sets, and a week that
    // starts on Sunday, which EventKit lets nothing but an import set.
    let imported = EKRecurrenceRule(recurrenceWith: .yearly, interval: 1, daysOfTheWeek: [EKRecurrenceDayOfWeek(.monday)],
        daysOfTheMonth: [1, -1], monthsOfTheYear: [3, 9], weeksOfTheYear: [10], daysOfTheYear: [100],
        setPositions: [1], end: nil)
    imported.setValue(1, forKey: "firstDayOfTheWeek")
    let full = EKEvent(eventStore: store)
    full.recurrenceRules = [imported]
    repeats["imported"] = repeatItem(full) ?? NSNull()
    // Two rules, which only Calendar makes, both read back.
    let both = EKEvent(eventStore: store)
    both.recurrenceRules = [EKRecurrenceRule(recurrenceWith: .weekly, interval: 1, end: nil),
                            EKRecurrenceRule(recurrenceWith: .monthly, interval: 3, end: nil)]
    repeats["two_rules"] = repeatItem(both) ?? NSNull()
    // A reminder's repeat set, changed and stopped; its due date cleared while it repeats is
    // refused, and with the repeat stopped too it is not.
    var reminders: [String: Any] = [:]
    let changes: [(String, [String: Any])] = [
        ("set", ["due": "2031-01-19T09:00", "repeat": ["every": "month"]]),
        ("changed", ["repeat": ["every": "week", "on": ["monday"]]]),
        ("stopped", ["repeat": ["every": "never"]]),
        ("due_cleared", ["due": ""]),
        ("due_cleared_and_stopped", ["due": "", "repeat": ["every": "never"]])]
    for (name, args) in changes {
        // Each starts due 2031-01-05 at 08:00 and repeating every day, except the first, which has neither.
        let r = EKReminder(eventStore: store)
        if name != "set" {
            due(["due": "2031-01-05T08:00"])?.apply(to: r)
            r.recurrenceRules = [EKRecurrenceRule(recurrenceWith: .daily, interval: 1, end: nil)]
        }
        let problem = reschedule(r, due: due(args), rule: repetition(args))
        reminders[name] = ["refused": problem as Any? ?? NSNull(), "repeat": repeatItem(r) as Any? ?? NSNull(),
                           "due": r.dueDateComponents != nil]
    }
    let data = try! JSONSerialization.data(withJSONObject: ["due": dues, "repeat": repeats, "reminders": reminders],
                                           options: [.sortedKeys])
    print(String(data: data, encoding: .utf8)!)
}

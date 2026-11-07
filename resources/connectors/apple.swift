// OPPA connector for the Mac's Calendar, Reminders and Contacts (connector protocol v1).
// Reads one JSON request on stdin and writes one JSON object on stdout. Reads carry
// each item's id; the write operations create, update and delete by that id, and the
// broker offers them only to full-access runs. Full access to Calendars, Reminders
// and Contacts covers writing. Access is asked for only when the request allows a
// prompt, because a background service has nobody to answer one.
import Contacts
import EventKit
import Foundation

let maxItems = 200
let maxText = 1000
let store = EKEventStore()
let iso: ISO8601DateFormatter = {
    let f = ISO8601DateFormatter()
    f.formatOptions = [.withInternetDateTime]
    f.timeZone = TimeZone.current
    return f
}()

func emit(_ value: [String: Any]) -> Never {
    let data = try! JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write("\n".data(using: .utf8)!)
    exit(0)
}

func fail(_ error: String, fix: String? = nil) -> Never {
    var value: [String: Any] = ["status": "error", "error": error]
    if let fix = fix { value["fix"] = fix }
    emit(value)
}

func clip(_ text: String?) -> String? {
    guard let text = text, !text.isEmpty else { return nil }
    return text.count > maxText ? String(text.prefix(maxText)) : text
}

func statusName(_ s: EKAuthorizationStatus) -> String {
    switch s {
    case .notDetermined: return "not_determined"
    case .restricted: return "restricted"
    case .denied: return "denied"
    case .fullAccess: return "full_access"
    case .writeOnly: return "write_only"
    @unknown default: return "unknown"
    }
}

func contactsStatusName(_ s: CNAuthorizationStatus) -> String {
    switch s {
    case .notDetermined: return "not_determined"
    case .restricted: return "restricted"
    case .denied: return "denied"
    case .authorized: return "full_access"
    case .limited: return "limited"
    @unknown default: return "unknown"
    }
}

let fixText = "Allow the process running OPPA in System Settings > Privacy & Security > %@, or run `oppa connectors test --prompt` at the Mac and answer the prompt."

/// Returns when access is granted; otherwise reports a plain fix.
func requireAccess(_ entity: EKEntityType, prompt: Bool) {
    let name = entity == .event ? "Calendars" : "Reminders"
    let status = EKEventStore.authorizationStatus(for: entity)
    if status == .fullAccess { return }
    if status == .notDetermined && prompt {
        let done = DispatchSemaphore(value: 0)
        var granted = false
        let handler: (Bool, Error?) -> Void = { ok, _ in granted = ok; done.signal() }
        if entity == .event { store.requestFullAccessToEvents(completion: handler) }
        else { store.requestFullAccessToReminders(completion: handler) }
        if done.wait(timeout: .now() + 120) == .success && granted { return }
    }
    fail("\(name) access is \(statusName(EKEventStore.authorizationStatus(for: entity)))",
         fix: String(format: fixText, name))
}

func requireContacts(prompt: Bool) {
    let status = CNContactStore.authorizationStatus(for: .contacts)
    if status == .authorized { return }
    if status == .notDetermined && prompt {
        let done = DispatchSemaphore(value: 0)
        var granted = false
        CNContactStore().requestAccess(for: .contacts) { ok, _ in granted = ok; done.signal() }
        if done.wait(timeout: .now() + 120) == .success && granted { return }
    }
    fail("Contacts access is \(contactsStatusName(CNContactStore.authorizationStatus(for: .contacts)))",
         fix: String(format: fixText, "Contacts"))
}

let isoFraction: ISO8601DateFormatter = {
    let f = ISO8601DateFormatter()
    f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return f
}()

/// A date and time written without an offset, read in `zone`. Leniency is set explicitly:
/// Foundation otherwise reads February 30 as March 2.
func strictLocal(_ text: String, in zone: TimeZone = .current) -> Date? {
    let local = DateFormatter()
    local.locale = Locale(identifier: "en_US_POSIX")
    local.calendar = Calendar(identifier: .gregorian)
    local.timeZone = zone
    local.isLenient = false
    for format in ["yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd"] {
        local.dateFormat = format
        if let d = local.date(from: text) { return d }
    }
    return nil
}

/// `fallback` stands in only for an absent value; anything else must be a real date. The
/// RFC 3339 parser rolls impossible dates over too, so its fields are checked first.
func date(_ value: Any?, _ fallback: Date) -> Date {
    if value == nil || value is NSNull { return fallback }
    guard let text = value as? String else { fail("dates are strings, such as 2026-01-02T15:00") }
    let fields = text.replacingOccurrences(of: "(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})$", with: "",
                                           options: .regularExpression)
    if fields == text, let local = strictLocal(text) { return local }
    // With an offset the fields need only exist; UTC has no daylight-saving gap to reject them.
    if fields != text, strictLocal(fields, in: TimeZone(identifier: "UTC")!) != nil,
       let d = iso.date(from: text) ?? isoFraction.date(from: text) { return d }
    fail("dates are RFC 3339 times, local times like 2026-01-02T15:00, or YYYY-MM-DD days, and must exist: \(text)")
}

/// A date argument, or nil when it is absent.
func when(_ args: [String: Any], _ key: String) -> Date? {
    args[key] == nil || args[key] is NSNull ? nil : date(args[key], Date())
}

/// A list of names. One given as anything else is refused rather than read as all of them.
func names(_ args: [String: Any], _ key: String) -> [String]? {
    guard let value = args[key], !(value is NSNull) else { return nil }
    guard let names = value as? [String] else { fail("\(key) must be a list of text") }
    return names
}

func calendars(_ entity: EKEntityType, named names: [String]?) -> [EKCalendar] {
    let all = store.calendars(for: entity)
    guard let names = names, !names.isEmpty else { return all }
    let chosen = all.filter { names.contains($0.title) }
    if chosen.isEmpty { fail("no calendar or list named \(names.joined(separator: ", "))") }
    return chosen
}

func eventItem(_ e: EKEvent) -> [String: Any] {
    var item: [String: Any] = ["id": e.eventIdentifier ?? "", "title": e.title ?? "",
                               "start": iso.string(from: e.startDate), "end": iso.string(from: e.endDate),
                               "all_day": e.isAllDay, "calendar": e.calendar.title,
                               "calendar_id": e.calendar.calendarIdentifier]
    if let rule = repeatItem(e) { item["repeat"] = rule }
    if let place = clip(e.location) { item["location"] = place }
    if let notes = clip(e.notes) { item["notes"] = notes }
    return item
}

func calendarEvents(_ args: [String: Any], prompt: Bool) -> Never {
    let start = date(args["from"], Calendar.current.startOfDay(for: Date()))
    let end = date(args["to"], Calendar.current.date(byAdding: .day, value: 1, to: start) ?? start.addingTimeInterval(86_400))
    if end <= start || end.timeIntervalSince(start) > 62 * 86_400 {
        fail("the range must end after it starts and span at most 62 days")
    }
    let named = names(args, "calendars")
    requireAccess(.event, prompt: prompt)
    let chosen = calendars(.event, named: named)
    let events = store.events(matching: store.predicateForEvents(withStart: start, end: end, calendars: chosen))
        .sorted { $0.startDate < $1.startDate }
    let items: [[String: Any]] = events.prefix(maxItems).map { e in
        eventItem(e)
    }
    emit(["status": "observed", "result": ["from": iso.string(from: start), "to": iso.string(from: end),
          "time_zone": TimeZone.current.identifier, "calendars": chosen.map { $0.title },
          "events": items, "truncated": events.count > maxItems]])
}

func calendarList(prompt: Bool) -> Never {
    requireAccess(.event, prompt: prompt)
    let items = store.calendars(for: .event).map { c -> [String: Any] in
        ["id": c.calendarIdentifier, "title": c.title, "account": c.source.title,
         "writable": c.allowsContentModifications]
    }
    emit(["status": "observed", "result": ["calendars": items]])
}

/// Reminders whose title contains the query, completed or not, so a reminder can be
/// found again after it was checked off.
func remindersFind(_ args: [String: Any], prompt: Bool) -> Never {
    let query = text(args, "query", required: true)!
    let named = names(args, "lists")
    requireAccess(.reminder, prompt: prompt)
    let chosen = calendars(.reminder, named: named)
    let done = DispatchSemaphore(value: 0)
    var found: [EKReminder] = []
    store.fetchReminders(matching: store.predicateForReminders(in: chosen)) { reminders in found = reminders ?? []; done.signal() }
    if done.wait(timeout: .now() + 30) == .timedOut { fail("Reminders did not answer within 30 seconds") }
    let hits = found.filter { ($0.title ?? "").localizedCaseInsensitiveContains(query) }
    emit(["status": "observed", "result": ["reminders": hits.prefix(50).map(reminderItem), "truncated": hits.count > 50]])
}

func remindersDue(_ args: [String: Any], prompt: Bool) -> Never {
    let before = when(args, "due_before")
    let named = names(args, "lists")
    requireAccess(.reminder, prompt: prompt)
    let chosen = calendars(.reminder, named: named)
    let predicate = store.predicateForIncompleteReminders(withDueDateStarting: nil, ending: before, calendars: chosen)
    let done = DispatchSemaphore(value: 0)
    var found: [EKReminder] = []
    store.fetchReminders(matching: predicate) { reminders in found = reminders ?? []; done.signal() }
    if done.wait(timeout: .now() + 30) == .timedOut { fail("Reminders did not answer within 30 seconds") }
    func due(_ r: EKReminder) -> Date? {
        guard let parts = r.dueDateComponents else { return nil }
        return parts.date ?? Calendar.current.date(from: parts)
    }
    let dated = found.sorted { (due($0) ?? .distantFuture) < (due($1) ?? .distantFuture) }
    let items = dated.prefix(maxItems).map(reminderItem)
    emit(["status": "observed", "result": ["lists": chosen.map { ["id": $0.calendarIdentifier, "title": $0.title] }, "reminders": items,
          "truncated": found.count > maxItems, "time_zone": TimeZone.current.identifier]])
}

func contactsSearch(_ args: [String: Any], prompt: Bool) -> Never {
    let query = text(args, "query", required: true)!
    requireContacts(prompt: prompt)
    let keys: [CNKeyDescriptor] = [CNContactFormatter.descriptorForRequiredKeys(for: .fullName),
                                   CNContactPhoneNumbersKey as CNKeyDescriptor, CNContactEmailAddressesKey as CNKeyDescriptor,
                                   CNContactOrganizationNameKey as CNKeyDescriptor, CNContactBirthdayKey as CNKeyDescriptor]
    let found: [CNContact]
    do {
        found = try CNContactStore().unifiedContacts(matching: CNContact.predicateForContacts(matchingName: query), keysToFetch: keys)
    } catch {
        fail("Contacts could not be searched")
    }
    let items: [[String: Any]] = found.prefix(20).map { c in
        var item: [String: Any] = ["id": c.identifier, "name": CNContactFormatter.string(from: c, style: .fullName) ?? "",
            "phones": c.phoneNumbers.map { ["label": CNLabeledValue<NSString>.localizedString(forLabel: $0.label ?? ""), "number": $0.value.stringValue] },
            "emails": c.emailAddresses.map { $0.value as String }]
        if !c.organizationName.isEmpty { item["organization"] = c.organizationName }
        if let b = c.birthday, let m = b.month, let d = b.day { item["birthday"] = String(format: "%02d-%02d", m, d) }
        return item
    }
    emit(["status": "observed", "result": ["contacts": items, "truncated": found.count > 20]])
}

// MARK: Writes
//
// Each write checks all of its arguments before it asks for access, so a bad
// request never reaches Calendar, Reminders or Contacts.

/// A text argument. One that is present but not text is refused, so a mistyped
/// calendar or list never falls back to the default.
func text(_ args: [String: Any], _ key: String, required: Bool = false) -> String? {
    if let value = args[key], !(value is NSNull), !(value is String) { fail("\(key) must be text") }
    if let value = args[key] as? String, !value.trimmingCharacters(in: .whitespaces).isEmpty {
        return value.count > 4000 ? String(value.prefix(4000)) : value
    }
    if required { fail("\(key) is required") }
    return nil
}

/// A text argument that can clear a field: nil when absent, nil inside when given empty.
func change(_ args: [String: Any], _ key: String) -> String?? {
    guard let value = args[key], !(value is NSNull) else { return .none }
    guard value is String else { fail("\(key) must be text") }
    return .some(text(args, key))
}

/// Whether JSON wrote the value as true or false. Foundation reads both booleans and
/// numbers as NSNumber, so 1 would otherwise pass for true and true for 1.
func isBoolean(_ value: Any) -> Bool {
    CFGetTypeID(value as CFTypeRef) == CFBooleanGetTypeID()
}

func flag(_ args: [String: Any], _ key: String) -> Bool? {
    guard let value = args[key], !(value is NSNull) else { return nil }
    guard isBoolean(value), let flag = value as? Bool else { fail("\(key) must be true or false") }
    return flag
}

func number(_ args: [String: Any], _ key: String) -> Int? {
    guard let value = args[key], !(value is NSNull) else { return nil }
    guard !isBoolean(value), let number = value as? Int else { fail("\(key) must be a whole number") }
    return number
}

let weekdays = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
let frequencies: [String: EKRecurrenceFrequency] = ["day": .daily, "week": .weekly, "month": .monthly, "year": .yearly]

/// A repeat argument, or nil when absent: the rule to set, or none to stop repeating.
/// A day as its last date includes all of that day.
func repetition(_ args: [String: Any]) -> EKRecurrenceRule?? {
    guard let value = args["repeat"], !(value is NSNull) else { return .none }
    guard let given = value as? [String: Any] else { fail("repeat must be an object, such as {\"every\": \"month\"}") }
    if !Set(given.keys).isSubset(of: ["every", "interval", "on", "until"]) { fail("repeat takes every, interval, on and until") }
    let every = text(given, "every", required: true)!
    if every == "never" {
        if given.count > 1 { fail("repeat every never takes nothing else") }
        return .some(nil)
    }
    guard let frequency = frequencies[every] else { fail("repeat every is day, week, month, year or never") }
    let interval = number(given, "interval") ?? 1
    if !(1...99).contains(interval) { fail("repeat interval is 1 to 99") }
    let days = names(given, "on")?.map { name -> EKRecurrenceDayOfWeek in
        guard let day = weekdays.firstIndex(of: name) else { fail("repeat on takes day names, such as monday") }
        return EKRecurrenceDayOfWeek(EKWeekday(rawValue: day + 1)!)
    }
    if let days = days {
        if frequency != .weekly { fail("repeat on names days only for every week") }
        if days.isEmpty || Set(days.map { $0.dayOfTheWeek }).count != days.count { fail("repeat on names each day once") }
    }
    let end = when(given, "until").map { last -> EKRecurrenceEnd in
        let day = (given["until"] as? String)?.count == 10
        return EKRecurrenceEnd(end: day ? Calendar.current.date(byAdding: DateComponents(day: 1, second: -1), to: last)! : last)
    }
    return .some(EKRecurrenceRule(recurrenceWith: frequency, interval: interval, daysOfTheWeek: days,
                                  daysOfTheMonth: nil, monthsOfTheYear: nil, weeksOfTheYear: nil,
                                  daysOfTheYear: nil, setPositions: nil, end: end))
}

/// How an item repeats, whole, including rules made elsewhere that no write here can make.
/// EventKit keeps an item's rules in no order, so they read back by frequency and then
/// interval; any after the first, which only Calendar makes, under `also`.
func repeatItem(_ item: EKCalendarItem) -> [String: Any]? {
    guard let rules = item.recurrenceRules?.sorted(by: {
        ($0.frequency.rawValue, $0.interval) < ($1.frequency.rawValue, $1.interval)
    }), let first = rules.first else { return nil }
    var value = ruleItem(first)
    if rules.count > 1 { value["also"] = rules.dropFirst().map(ruleItem) }
    return value
}

func ruleItem(_ rule: EKRecurrenceRule) -> [String: Any] {
    var value: [String: Any] = ["every": frequencies.first { $0.value == rule.frequency }?.key ?? "other",
                                "interval": rule.interval]
    if let days = rule.daysOfTheWeek {
        value["on"] = days.map { ($0.weekNumber == 0 ? "" : "\($0.weekNumber) ") + weekdays[$0.dayOfTheWeek.rawValue - 1] }
    }
    if let days = rule.daysOfTheMonth { value["days_of_month"] = days }
    if let months = rule.monthsOfTheYear { value["months"] = months }
    if let weeks = rule.weeksOfTheYear { value["weeks_of_year"] = weeks }
    if let days = rule.daysOfTheYear { value["days_of_year"] = days }
    if let positions = rule.setPositions { value["positions"] = positions }
    if let end = rule.recurrenceEnd {
        if let last = end.endDate { value["until"] = iso.string(from: last) } else { value["times"] = end.occurrenceCount }
    }
    // A week that starts on another day than Monday, the iCalendar default, moves an
    // every-other-week rule's dates.
    if ![0, 2].contains(rule.firstDayOfTheWeek) { value["week_starts"] = weekdays[rule.firstDayOfTheWeek - 1] }
    return value
}

func writable(_ entity: EKEntityType, named: String?) -> EKCalendar {
    let fallback = entity == .event ? store.defaultCalendarForNewEvents : store.defaultCalendarForNewReminders()
    guard let name = named else {
        guard let calendar = fallback else { fail("there is no default \(entity == .event ? "calendar" : "list")") }
        return calendar
    }
    let all = store.calendars(for: entity)
    let titled = all.filter { $0.title == name }
    if titled.count > 1 { fail("\(titled.count) calendars or lists are named \(name); give the id a read returned") }
    guard let calendar = all.first(where: { $0.calendarIdentifier == name }) ?? titled.first else {
        fail("no calendar or list is named \(name) or has that id")
    }
    if !calendar.allowsContentModifications { fail("\(name) is read-only") }
    return calendar
}

/// The event an argument names: its id and, for one occurrence of a repeating
/// event, that occurrence's start.
typealias Occurrence = (id: String, start: Date?)

func occurrence(_ args: [String: Any]) -> Occurrence {
    (text(args, "id", required: true)!, when(args, "occurrence_start"))
}

/// The event an id names; for a repeating event, the occurrence that starts at
/// the given start, or the first one.
func event(_ target: Occurrence) -> EKEvent {
    guard let first = store.event(withIdentifier: target.id) else { fail("no event has that id") }
    guard let start = target.start else { return first }
    let found = store.events(matching: store.predicateForEvents(withStart: start.addingTimeInterval(-60),
        end: start.addingTimeInterval(86_400), calendars: [first.calendar]))
    guard let match = found.first(where: { $0.eventIdentifier == target.id && abs($0.startDate.timeIntervalSince(start)) < 60 }) else {
        fail("that event has no occurrence starting at \(iso.string(from: start)); an occurrence changed on its own has its own id, so read the events again")
    }
    return match
}

func span(_ args: [String: Any]) -> EKSpan {
    switch text(args, "span") {
    case .none, "this"?: return .thisEvent
    case "future"?: return .futureEvents
    default: fail("span is this or future")
    }
}

func saveEvent(_ e: EKEvent, _ span: EKSpan) -> Never {
    if e.endDate < e.startDate { fail("the event must end after it starts") }
    do { try store.save(e, span: span, commit: true) } catch { fail("Calendar did not save the event: \(error.localizedDescription)") }
    emit(["status": "observed", "result": ["event": eventItem(e)]])
}

func calendarCreate(_ args: [String: Any]) -> Never {
    let title = text(args, "title", required: true)
    guard let start = when(args, "start") else { fail("start is required") }
    let end = when(args, "end") ?? start.addingTimeInterval(3600)
    if end < start { fail("the event must end after it starts") }
    let allDay = flag(args, "all_day") ?? false
    let name = text(args, "calendar"), location = text(args, "location"), notes = text(args, "notes")
    let rule = repetition(args) ?? nil
    requireAccess(.event, prompt: false)
    let e = EKEvent(eventStore: store)
    e.title = title
    e.startDate = start
    e.endDate = end
    e.isAllDay = allDay
    e.calendar = writable(.event, named: name)
    e.location = location
    e.notes = notes
    e.recurrenceRules = rule.map { [$0] }
    saveEvent(e, rule == nil ? .thisEvent : .futureEvents)
}

func calendarUpdate(_ args: [String: Any]) -> Never {
    let target = occurrence(args), scope = span(args)
    let title = text(args, "title"), name = text(args, "calendar")
    let start = when(args, "start"), end = when(args, "end")
    let allDay = flag(args, "all_day")
    let location = change(args, "location"), notes = change(args, "notes")
    // How an event repeats belongs to the occurrence it changes at and every later one.
    let rule = repetition(args)
    if rule != nil && scope == .thisEvent && args["span"] != nil {
        fail("a change to repeat applies to this occurrence and later ones; give span future")
    }
    requireAccess(.event, prompt: false)
    let e = event(target)
    if !e.calendar.allowsContentModifications { fail("\(e.calendar.title) is read-only") }
    if let title = title { e.title = title }
    if let start = start { e.startDate = start }
    if let end = end { e.endDate = end }
    if let allDay = allDay { e.isAllDay = allDay }
    if let name = name { e.calendar = writable(.event, named: name) }
    if let location = location { e.location = location }
    if let notes = notes { e.notes = notes }
    if let rule = rule { e.recurrenceRules = rule.map { [$0] } }
    saveEvent(e, rule == nil ? scope : .futureEvents)
}

func calendarDelete(_ args: [String: Any]) -> Never {
    let target = occurrence(args), scope = span(args)
    requireAccess(.event, prompt: false)
    let e = event(target)
    let item = eventItem(e)
    do { try store.remove(e, span: scope, commit: true) } catch { fail("Calendar did not delete the event: \(error.localizedDescription)") }
    emit(["status": "observed", "result": ["deleted": item]])
}

func reminder(_ id: String) -> EKReminder {
    guard let r = store.calendarItem(withIdentifier: id) as? EKReminder else { fail("no reminder has that id") }
    return r
}

func reminderItem(_ r: EKReminder) -> [String: Any] {
    var item: [String: Any] = ["id": r.calendarItemIdentifier, "title": r.title ?? "", "list": r.calendar.title,
                               "list_id": r.calendar.calendarIdentifier, "priority": r.priority,
                               "completed": r.isCompleted]
    if let parts = r.dueDateComponents, let when = parts.date ?? Calendar.current.date(from: parts) {
        item["due"] = iso.string(from: when)
    }
    if let notes = clip(r.notes) { item["notes"] = notes }
    if let alert = r.alarms?.compactMap({ $0.absoluteDate }).first { item["alert"] = iso.string(from: alert) }
    if let rule = repeatItem(r) { item["repeat"] = rule }
    return item
}

/// A reminder's due date and the one alert that goes with it. A due date alone does not
/// notify the owner's devices, so a due time also sets an alert at that time; a day only,
/// or an empty value, sets none.
struct Due {
    let components: DateComponents?
    let alert: Date?

    func apply(to r: EKReminder) {
        r.dueDateComponents = components
        r.alarms = alert.map { [EKAlarm(absoluteDate: $0)] }
    }
}

/// A reminder's due argument, or nil when absent. Empty text clears it; a YYYY-MM-DD
/// day sets a date without a time.
func due(_ args: [String: Any]) -> Due? {
    guard let given = change(args, "due") else { return nil }
    guard let day = given else { return Due(components: nil, alert: nil) }
    let dayOnly = day.count == 10
    let units: Set<Calendar.Component> = dayOnly ? [.year, .month, .day] : [.year, .month, .day, .hour, .minute]
    let instant = date(day, Date())
    // The alert is the instant asked for, to the minute, as the reminder keeps its due time;
    // local components alone cannot tell apart the two 1:30s of the night clocks go back.
    let minute = Date(timeIntervalSinceReferenceDate: (instant.timeIntervalSinceReferenceDate / 60).rounded(.down) * 60)
    return Due(components: Calendar.current.dateComponents(units, from: instant), alert: dayOnly ? nil : minute)
}

func saveReminder(_ r: EKReminder) -> Never {
    do { try store.save(r, commit: true) } catch { fail("Reminders did not save the reminder: \(error.localizedDescription)") }
    emit(["status": "observed", "result": ["reminder": reminderItem(r)]])
}

func remindersCreate(_ args: [String: Any]) -> Never {
    let title = text(args, "title", required: true)
    let name = text(args, "list"), notes = text(args, "notes")
    let priority = number(args, "priority"), dueAt = due(args), rule = repetition(args) ?? nil
    if rule != nil && dueAt?.components == nil { fail("a repeating reminder needs a due date") }
    requireAccess(.reminder, prompt: false)
    let r = EKReminder(eventStore: store)
    r.title = title
    r.calendar = writable(.reminder, named: name)
    r.notes = notes
    if let priority = priority { r.priority = max(0, min(9, priority)) }
    dueAt?.apply(to: r)
    r.recurrenceRules = rule.map { [$0] }
    saveReminder(r)
}

func remindersUpdate(_ args: [String: Any]) -> Never {
    let id = text(args, "id", required: true)!
    let title = text(args, "title"), name = text(args, "list"), notes = change(args, "notes")
    let priority = number(args, "priority"), completed = flag(args, "completed"), dueAt = due(args)
    let rule = repetition(args)
    requireAccess(.reminder, prompt: false)
    let r = reminder(id)
    if let title = title { r.title = title }
    if let name = name { r.calendar = writable(.reminder, named: name) }
    if let notes = notes { r.notes = notes }
    if let priority = priority { r.priority = max(0, min(9, priority)) }
    if let completed = completed { r.isCompleted = completed }
    if let problem = reschedule(r, due: dueAt, rule: rule) { fail(problem) }
    saveReminder(r)
}

/// Apply a change to a reminder's due date and repeat, and say what is wrong with the result,
/// if anything: a repeating reminder needs a due date, so one cleared while the reminder still
/// repeats is refused before anything is saved.
func reschedule(_ r: EKReminder, due: Due?, rule: EKRecurrenceRule??) -> String? {
    due?.apply(to: r)
    if let rule = rule { r.recurrenceRules = rule.map { [$0] } }
    return r.hasRecurrenceRules && r.dueDateComponents == nil
        ? "a repeating reminder needs a due date; to clear it, also give repeat every never" : nil
}

func remindersDelete(_ args: [String: Any]) -> Never {
    let id = text(args, "id", required: true)!
    requireAccess(.reminder, prompt: false)
    let r = reminder(id)
    let item = reminderItem(r)
    do { try store.remove(r, commit: true) } catch { fail("Reminders did not delete the reminder: \(error.localizedDescription)") }
    emit(["status": "observed", "result": ["deleted": item]])
}

let contactKeys: [CNKeyDescriptor] = [CNContactFormatter.descriptorForRequiredKeys(for: .fullName),
    CNContactGivenNameKey as CNKeyDescriptor, CNContactFamilyNameKey as CNKeyDescriptor,
    CNContactPhoneNumbersKey as CNKeyDescriptor, CNContactEmailAddressesKey as CNKeyDescriptor,
    CNContactOrganizationNameKey as CNKeyDescriptor]

func contactItem(_ c: CNContact) -> [String: Any] {
    var item: [String: Any] = ["id": c.identifier, "name": CNContactFormatter.string(from: c, style: .fullName) ?? "",
        "phones": c.phoneNumbers.map { $0.value.stringValue }, "emails": c.emailAddresses.map { $0.value as String }]
    if !c.organizationName.isEmpty { item["organization"] = c.organizationName }
    return item
}

/// What a contact request changes, read before Contacts is touched. A name or
/// organization given as empty text clears it.
struct ContactChange {
    let given: String??, family: String??, organization: String??
    let phones: [String], emails: [String]

    init(_ args: [String: Any]) {
        given = change(args, "given_name")
        family = change(args, "family_name")
        organization = change(args, "organization")
        phones = names(args, "add_phones") ?? []
        emails = names(args, "add_emails") ?? []
    }

    var named: Bool { [given, family, organization].contains { ($0 ?? nil) != nil } }

    func apply(to c: CNMutableContact) {
        if let given = given { c.givenName = given ?? "" }
        if let family = family { c.familyName = family ?? "" }
        if let organization = organization { c.organizationName = organization ?? "" }
        for number in phones {
            c.phoneNumbers.append(CNLabeledValue(label: CNLabelPhoneNumberMobile, value: CNPhoneNumber(stringValue: number)))
        }
        for email in emails {
            c.emailAddresses.append(CNLabeledValue(label: CNLabelHome, value: email as NSString))
        }
    }
}

func saveContacts(_ request: CNSaveRequest, _ what: String) {
    do { try CNContactStore().execute(request) } catch { fail("Contacts did not \(what): \(error.localizedDescription)") }
}

func existingContact(_ id: String) -> CNContact {
    guard let c = try? CNContactStore().unifiedContact(withIdentifier: id, keysToFetch: contactKeys) else {
        fail("no contact has that id")
    }
    return c
}

func contactsCreate(_ args: [String: Any]) -> Never {
    let change = ContactChange(args)
    if !change.named { fail("a contact needs a name or an organization") }
    requireContacts(prompt: false)
    let c = CNMutableContact()
    change.apply(to: c)
    let request = CNSaveRequest()
    request.add(c, toContainerWithIdentifier: nil)
    saveContacts(request, "save the contact")
    emit(["status": "observed", "result": ["contact": contactItem(c)]])
}

func contactsUpdate(_ args: [String: Any]) -> Never {
    let id = text(args, "id", required: true)!, change = ContactChange(args)
    requireContacts(prompt: false)
    guard let c = existingContact(id).mutableCopy() as? CNMutableContact else { fail("the contact cannot be edited") }
    change.apply(to: c)
    let request = CNSaveRequest()
    request.update(c)
    saveContacts(request, "save the contact")
    emit(["status": "observed", "result": ["contact": contactItem(c)]])
}

func contactsDelete(_ args: [String: Any]) -> Never {
    let id = text(args, "id", required: true)!
    requireContacts(prompt: false)
    let found = existingContact(id)
    guard let c = found.mutableCopy() as? CNMutableContact else { fail("the contact cannot be deleted") }
    let request = CNSaveRequest()
    request.delete(c)
    saveContacts(request, "delete the contact")
    emit(["status": "observed", "result": ["deleted": contactItem(found)]])
}

/// Answer the one request on stdin. A test build (-D TESTING, with this file as
/// main.swift) runs tests/apple/checks.swift's checks instead.
func main() -> Never {
    let input = FileHandle.standardInput.readDataToEndOfFile()
    guard let request = (try? JSONSerialization.jsonObject(with: input)) as? [String: Any],
          request["protocol_version"] as? Int == 1, let method = request["method"] as? String else {
        fail("invalid connector request")
    }
    let prompt = request["may_prompt"] as? Bool ?? false
    switch method {
    case "describe":
        func writeOperations(_ range: [String: Any]) -> [[String: Any]] {
            let string: [String: Any] = ["type": "string", "maxLength": 4000]
            let id: [String: Any] = ["type": "string", "minLength": 1, "maxLength": 256, "description": "The id a read returned"]
            let occurrence: [String: Any] = ["occurrence_start": range,
                "span": ["type": "string", "enum": ["this", "future"], "description": "For a repeating event: this occurrence (default) or it and later ones"]]
            let calendar: [String: Any] = ["type": "string", "maxLength": 256,
                                           "description": "A calendar or list id from a read, or its name when no other has it"]
            let repeats: [String: Any] = ["type": "object", "additionalProperties": false, "required": ["every"],
                "description": "Repeat from the start or due date; every never stops repeating",
                "properties": ["every": ["type": "string", "enum": ["day", "week", "month", "year", "never"]],
                               "interval": ["type": "integer", "minimum": 1, "maximum": 99, "description": "Every this many; 1 when absent"],
                               "on": ["type": "array", "items": ["type": "string", "enum": weekdays], "minItems": 1, "maxItems": 7,
                                      "uniqueItems": true, "description": "For every week: the days it falls on"],
                               "until": ["type": "string", "description": "The last day or time it may fall on"]]]
            let event: [String: Any] = ["title": string, "start": range, "end": range, "all_day": ["type": "boolean"],
                                        "calendar": calendar, "location": string, "notes": string, "repeat": repeats]
            let reminder: [String: Any] = ["title": string, "due": ["type": "string", "description": "RFC 3339 time or YYYY-MM-DD day; a time also sets an alert then; empty clears both"],
                                           "list": calendar, "notes": string, "priority": ["type": "integer", "minimum": 0, "maximum": 9],
                                           "repeat": repeats]
            let names: [String: Any] = ["type": "array", "items": ["type": "string", "maxLength": 320], "maxItems": 10]
            let contact: [String: Any] = ["given_name": string, "family_name": string, "organization": string,
                                          "add_phones": names, "add_emails": names]
            func op(_ name: String, _ tier: String, _ description: String, _ properties: [String: Any], _ required: [String]) -> [String: Any] {
                ["name": name, "tier": tier, "description": description,
                 "input_schema": ["type": "object", "additionalProperties": false, "required": required, "properties": properties]]
            }
            return [
                op("calendar_create_event", "reversible", "Create an event; defaults to the default calendar and one hour.",
                   event, ["title", "start"]),
                op("calendar_update_event", "reversible", "Change an event by id; only the given fields change; an empty location or notes clears it; a change to repeat applies to the occurrence and later ones.",
                   event.merging(occurrence) { a, _ in a }.merging(["id": id]) { a, _ in a }, ["id"]),
                op("calendar_delete_event", "outward", "Delete an event by id. Invitees may be notified.",
                   occurrence.merging(["id": id]) { a, _ in a }, ["id"]),
                op("reminders_create", "reversible", "Create a reminder; defaults to the default list; one that repeats needs a due date.", reminder, ["title"]),
                op("reminders_update", "reversible", "Change a reminder by id, including marking it completed.",
                   reminder.merging(["id": id, "completed": ["type": "boolean"]]) { a, _ in a }, ["id"]),
                op("reminders_delete", "outward", "Delete a reminder by id.", ["id": id], ["id"]),
                op("contacts_create", "reversible", "Create a contact with a name or organization, phones and emails.", contact, []),
                op("contacts_update", "reversible", "Change a contact's name or organization, or add phones and emails, by id.",
                   contact.merging(["id": id]) { a, _ in a }, ["id"]),
                op("contacts_delete", "outward", "Delete a contact by id.", ["id": id], ["id"]),
            ]
        }
        let range: [String: Any] = ["type": "string", "description": "RFC 3339 time, a local time like 2026-01-02T15:00, or a YYYY-MM-DD day, in the Mac's time zone"]
        let names: [String: Any] = ["type": "array", "items": ["type": "string"], "maxItems": 32]
        emit(["protocol_version": 1, "operations": [
            ["name": "calendar_events", "tier": "read",
             "description": "Events on the Mac's calendars (iCloud, Google and others added in Calendar) between two times; defaults to today. At most 62 days and 200 events.",
             "input_schema": ["type": "object", "additionalProperties": false,
                              "properties": ["from": range, "to": range, "calendars": names]]],
            ["name": "calendar_list", "tier": "read", "description": "Calendars the Mac can read, with their accounts.",
             "input_schema": ["type": "object", "additionalProperties": false, "properties": [:]]],
            ["name": "reminders_due", "tier": "read",
             "description": "Incomplete reminders, soonest due first, and every list read with its id; optionally only those due before a time or in named lists.",
             "input_schema": ["type": "object", "additionalProperties": false,
                              "properties": ["due_before": range, "lists": names]]],
            ["name": "reminders_find", "tier": "read",
             "description": "Reminders whose title contains the query, completed or not, optionally in named lists.",
             "input_schema": ["type": "object", "additionalProperties": false, "required": ["query"],
                              "properties": ["query": ["type": "string", "minLength": 1, "maxLength": 200], "lists": names]]],
            ["name": "contacts_search", "tier": "read", "description": "Contacts whose name matches, with phones, emails and birthday.",
             "input_schema": ["type": "object", "additionalProperties": false, "required": ["query"],
                              "properties": ["query": ["type": "string", "minLength": 1, "maxLength": 200]]]],
        ] + writeOperations(range)])
    case "health":
        if prompt {
            for entity in [EKEntityType.event, .reminder] where EKEventStore.authorizationStatus(for: entity) == .notDetermined {
                let done = DispatchSemaphore(value: 0)
                let handler: (Bool, Error?) -> Void = { _, _ in done.signal() }
                if entity == .event { store.requestFullAccessToEvents(completion: handler) }
                else { store.requestFullAccessToReminders(completion: handler) }
                _ = done.wait(timeout: .now() + 120)
            }
            if CNContactStore.authorizationStatus(for: .contacts) == .notDetermined {
                let done = DispatchSemaphore(value: 0)
                CNContactStore().requestAccess(for: .contacts) { _, _ in done.signal() }
                _ = done.wait(timeout: .now() + 120)
            }
        }
        let calendarsStatus = statusName(EKEventStore.authorizationStatus(for: .event))
        let remindersStatus = statusName(EKEventStore.authorizationStatus(for: .reminder))
        let contactsStatus = contactsStatusName(CNContactStore.authorizationStatus(for: .contacts))
        let missing = [("Calendars", calendarsStatus), ("Reminders", remindersStatus), ("Contacts", contactsStatus)]
            .filter { $0.1 != "full_access" }.map { $0.0 }
        var value: [String: Any] = ["ready": missing.isEmpty,
            "access": ["calendars": calendarsStatus, "reminders": remindersStatus, "contacts": contactsStatus]]
        if !missing.isEmpty {
            value["problem"] = "\(missing.joined(separator: ", ")) not allowed for this process"
            value["fix"] = String(format: fixText, missing.joined(separator: ", "))
        }
        emit(value)
    case "call":
        let args = request["arguments"] as? [String: Any] ?? [:]
        switch request["operation"] as? String ?? "" {
        case "calendar_events": calendarEvents(args, prompt: prompt)
        case "calendar_list": calendarList(prompt: prompt)
        case "reminders_due": remindersDue(args, prompt: prompt)
        case "reminders_find": remindersFind(args, prompt: prompt)
        case "contacts_search": contactsSearch(args, prompt: prompt)
        case "calendar_create_event": calendarCreate(args)
        case "calendar_update_event": calendarUpdate(args)
        case "calendar_delete_event": calendarDelete(args)
        case "reminders_create": remindersCreate(args)
        case "reminders_update": remindersUpdate(args)
        case "reminders_delete": remindersDelete(args)
        case "contacts_create": contactsCreate(args)
        case "contacts_update": contactsUpdate(args)
        case "contacts_delete": contactsDelete(args)
        default: fail("unknown operation")
        }
    default:
        fail("unknown method")
    }
}

#if TESTING
runTests()
#else
main()
#endif

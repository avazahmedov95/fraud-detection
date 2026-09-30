/* The "About the project" tab: what the system is made of (a click on a part opens
   its details), one decision of the running system walked through it with that
   decision's own stage times, the model and the rules. Nothing here is sample data.
   Loaded after index.html's script, whose helpers it uses (t, esc, fill, dec, pct,
   fmtInt, fmtMs, fmtTime, badge, last4, api, lang, status, results, selected, MUST). */

// The rules the job runs, in rules.py's order, with the numbers each one checks as
// stream-processor/config.py sets them. demo/tests pins both against the job.
const RULE_FACTS = {
  NEW_PAYEE_HIGH_AMOUNT: {NEW_PAYEE_ABS_FLOOR: 2000000, NEW_PAYEE_AMOUNT_FACTOR: 3},
  VELOCITY: {VELOCITY_MAX_COUNT: 5},
  STRUCTURING: {STRUCTURING_MIN_COUNT: 3, STRUCTURING_THRESHOLD: 10000000},
  DISTINCT_PAYEE_BURST: {DISTINCT_PAYEE_MAX: 5},
  GEO_ANOMALY: {},
  IMPOSSIBLE_TRAVEL: {MIN_TRAVEL_DISTANCE_KM: 100, MAX_PLAUSIBLE_KMH: 900},
  AMOUNT_DEVIATION: {AMOUNT_DEVIATION_SIGMA: 4, AMOUNT_DEVIATION_MIN_HISTORY: 5},
  COACHED_SESSION: {COACHED_SESSION_Z: 2},
  DAILY_LIMIT_BREACH: {LIMIT_DAILY: 100000000},
  MULE_FAN_IN: {MULE_FAN_IN_MIN_SENDERS: 6}
};
// The type an alert is named after: the first pattern whose rules fired
// (stream-processor/fusion.py _TYPE_PRIORITY; demo/tests pins it).
const NAMED_BY = [["STRUCTURING", ["STRUCTURING"]],
                  ["ATO", ["GEO_ANOMALY", "VELOCITY", "DISTINCT_PAYEE_BURST"]],
                  ["MULE", ["MULE_FAN_IN"]],
                  ["APP", ["NEW_PAYEE_HIGH_AMOUNT", "AMOUNT_DEVIATION"]]];

const GROUPS = ["in", "engine", "out", "offline"];
const COMPONENTS = [
  {id: "gen", group: "in"}, {id: "raw", group: "in"},
  {id: "decode", group: "engine"}, {id: "state", group: "engine"}, {id: "redis", group: "engine", store: true},
  {id: "rules", group: "engine"}, {id: "model", group: "engine", ml: true}, {id: "decide", group: "engine"},
  {id: "out", group: "out"}, {id: "sink", group: "out"}, {id: "ch", group: "out", store: true},
  {id: "neo", group: "out", store: true}, {id: "cases", group: "out"}, {id: "dash", group: "out"},
  {id: "train", group: "offline"}
];
// The path one transfer takes. `stage` names its time in the decision's stage_ms.
const WALK = [
  {at: "gen"}, {at: "raw", stage: "kafka"}, {at: "decode", stage: "handoff"}, {at: "decode", stage: "decode"},
  {at: "state", stage: "state"}, {at: "redis", stage: "redis"}, {at: "rules", stage: "rules"},
  {at: "model", stage: "model"}, {at: "decide", stage: "decide"}, {at: "out"}, {at: "sink"}, {at: "ch"},
  {at: "neo", alertOnly: true}, {at: "cases", alertOnly: true}, {at: "dash"}
];

const ABOUT = {
ru: {
  intro: "Система проверяет каждый P2P-перевод по картам в реальном времени: жёсткие правила и модель машинного обучения оценивают его за доли секунды, а подозрительные переводы уходят аналитику. Сама система ничего не блокирует — решает человек. Нажмите на любой компонент, чтобы прочитать о нём подробнее.",
  flow_title: "Из чего состоит система",
  group: {in: "Приём перевода", engine: "Flink: решение за миллисекунды", out: "После решения", offline: "Офлайн, не в живом пути"},
  parts: {
    gen: ["Генератор переводов", "Python", "откуда берутся переводы", [
      "Наш генератор создаёт реалистичные P2P-переводы по картам UzCard и HUMO: суммы, регионы, время, привычки людей.",
      "{rows} переводов за месяц, мошенничества {share}, четыре типа: мошенник по телефону, захват аккаунта, дроп-счёт, дробление сумм.",
      "Данные генерируются, потому что настоящих переводов Узбекистана в открытом доступе нет.",
      "В банке на этом месте стоял бы платёжный шлюз. Демо отправляет переводы из той части данных, которую модель не видела."]],
    raw: ["Kafka: входная очередь", "Apache Kafka", "transactions.raw", [
      "Надёжная очередь сообщений: переводы не теряются, даже если обработчик перезапускается.",
      "Ключ сообщения — карта отправителя: переводы одного человека идут по порядку к одному обработчику.",
      "Kafka ставит на каждый перевод своё время записи — по нему считается этап «во входную очередь»."]],
    decode: ["Чтение сообщения", "Flink", "первый шаг задачи", [
      "Задача Flink забирает перевод из очереди и распаковывает его; зашифрованный — расшифровывает.",
      "Сломанную запись (нет суммы, неверный формат) задача отбрасывает и считает, а не падает."]],
    state: ["История отправителя", "Flink state", "память о каждом отправителе", [
      "Flink хранит историю каждого отправителя у себя в памяти: обычные суммы, получатели, время переводов, регионы.",
      "Отсюда признаки вроде «сумма в 6 раз больше обычной» и «получатель новый».",
      "История переживает перезапуск: Flink регулярно сохраняет её на диск."]],
    redis: ["Сторона получателя", "Redis", "кто платит этому счёту", [
      "Redis хранит то, чего не видно из истории одного отправителя: сколько разных людей платили счёту за час, сутки и неделю и сколько денег пришло.",
      "Так ловится дроп-счёт: много людей платят одному счёту, и деньги сразу уходят дальше."]],
    rules: ["Признаки и правила", "Flink", "21 признак, 10 правил", [
      "21 признак: суммы и привычки отправителя, новизна получателя, скорость переводов, приток денег получателю, звонок во время подтверждения и другие.",
      "10 жёстких правил — известные схемы мошенничества. Подробно — в блоке «Правила» ниже."]],
    model: ["Модель", "LightGBM · ONNX", "оценка риска", [
      "Пять моделей градиентного бустинга на деревьях решений, их ответы усредняются.",
      "Работает внутри Flink как файл ONNX, без обращения к внешнему сервису.",
      "Подробно — в блоке «Модель» ниже."]],
    decide: ["Решение", "Flink", "пропустить или на проверку", [
      "Перевод идёт на проверку, если риск модели выше уровня тревоги или сработало обязательное правило регулятора.",
      "Система сама ничего не блокирует: подозрительный перевод решает человек.",
      "Правила дают тревоге название — например, «мошенник по телефону»."]],
    out: ["Kafka: решения и тревоги", "Apache Kafka", "transactions.scored, fraud.alerts", [
      "Каждое решение уходит в одну очередь, тревоги — ещё и в отдельную.",
      "Так запись в базу и работа аналитика не мешают друг другу."]],
    sink: ["Запись в базу", "Python", "sink-writer", [
      "Читает решения из Kafka и пачками записывает их в ClickHouse, а тревоги — в граф Neo4j.",
      "Если база недоступна, продолжает работать и считает, сколько записей потеряно."]],
    ch: ["ClickHouse", "ClickHouse", "хранилище", [
      "Каждое решение со временем всех этапов — по ним строится вкладка «Данные и результаты».",
      "Цепочка аудита: каждая запись связана с предыдущей хешем, поэтому незаметно изменить историю нельзя.",
      "Дела аналитика и его отметки."]],
    neo: ["Граф связей", "Neo4j", "кто кому платил", [
      "Тревоги в виде графа: счета — точки, переводы — стрелки.",
      "Кольцо дроп-счетов видно как много стрелок, сходящихся к одному счёту."]],
    cases: ["Разбор тревог", "Python", "case-manager", [
      "На каждую тревогу открывает дело с причинами словами — из вклада признаков в оценку модели.",
      "Аналитик отмечает: мошенничество или ложная тревога. Эти отметки — единственные настоящие метки в системе, на них банк переобучал бы модель."]],
    dash: ["Мониторинг", "Grafana · демо", "что видит банк", [
      "Grafana показывает число переводов и тревог, типы, распределение риска, регионы.",
      "Это демо показывает те же решения вживую."]],
    train: ["Обучение модели", "Python", "не в живом пути", [
      "Прогоняет тот же код признаков по данным генератора, поэтому признаки при обучении и в работе одинаковые.",
      "Обучает пять моделей LightGBM, подбирает уровень тревоги на отдельных данных и выгружает модель в ONNX.",
      "Подробно — в блоке «Модель» ниже."]]
  },
  close: "Закрыть",
  walk_title: "Путь одного перевода",
  walk_hint: "Выберите настоящее решение системы и нажмите «Показать путь»: перевод пройдёт по компонентам, на каждом шаге — его собственное время.",
  w_alert: "Последняя тревога", w_allow: "Обычный перевод", w_selected: "Выбранный на рабочем месте",
  w_play: "Показать путь", w_pause: "Пауза", w_prev: "◀ Назад", w_next: "Вперёд ▶",
  w_none: "Пока нет решений: запустите поток или покажите случай мошенничества на рабочем месте.",
  w_ready: "Нажмите «Показать путь» или «Вперёд».",
  m_stage: "этот этап", m_elapsed: "с прихода перевода",
  walk: {
    gen: ["Перевод отправлен", "Перевод {amount} сум с карты {from} на карту {to}. Он взят из данных нашего генератора и отправлен в систему."],
    raw: ["Во входную очередь", "Kafka записала перевод во входную очередь. Ключ — карта отправителя, поэтому вся история одного человека попадает к одному обработчику."],
    handoff: ["Ожидание передачи", "Flink забирает переводы из очереди и передаёт на обработку небольшими пачками. Это ожидание — почти всё время решения."],
    decode: ["Чтение", "Перевод распакован и проверен."],
    state: ["История отправителя", "Из памяти Flink прочитана история этого отправителя: обычные суммы, получатели, время прошлых переводов."],
    redis: ["Сторона получателя", "Из Redis прочитано, сколько разных людей платили этому счёту за час, сутки и неделю."],
    rules: ["Признаки и правила", "Посчитан 21 признак и проверены 10 правил. {rules}"],
    model: ["Модель", "Модель взвесила все 21 признак и оценила риск: {score}."],
    decide: ["Решение", "{decision}. {why}"],
    out: ["Решение отправлено", "Решение ушло в Kafka{alert}. От прихода перевода до решения прошло {total}."],
    sink: ["Запись в базу", "Служба записи забирает решения пачками. Решение уже принято, поэтому здесь спешить не нужно."],
    ch: ["ClickHouse", "В базе сохранены решение, время каждого этапа и запись в цепочке аудита."],
    neo: ["Граф связей", "Тревога стала связью в графе Neo4j: кто кому платил."],
    neo_allow: ["Граф связей", "Обычный перевод в граф не попадает: там только тревоги."],
    cases: ["Аналитик", "Открыто дело с причинами словами{reason}. Аналитик отметит: мошенничество или ложная тревога."],
    cases_allow: ["Аналитик", "Тревоги нет, поэтому аналитик этот перевод не увидит."],
    dash: ["Мониторинг", "Grafana и это демо показывают весь поток: число переводов, тревоги, типы и время."]
  },
  fired: "Сработали: {list}.", none_fired: "Ни одно правило не сработало.",
  why_must: "Сработало обязательное правило регулятора — такой перевод всегда идёт на проверку.",
  why_review: "Риск {score} выше уровня тревоги {cut}.",
  why_allow: "Риск {score} ниже уровня тревоги {cut}, и ни одно правило не требует проверки.",
  alert_too: ", а тревога — ещё и в очередь тревог",
  reason: "; главная причина — «{label}»",
  model_title: "Модель",
  model_lines: [
    "Пять моделей LightGBM — градиентный бустинг на деревьях решений; их ответы усредняются.",
    "Смотрит на 21 признак: суммы и привычки отправителя, новизну получателя, скорость переводов, приток денег получателю, звонок во время подтверждения.",
    "Обучена на первых {fit} переводах генератора по времени. Уровень тревоги {cut} подобран на следующих {val}.",
    "Проверена на последних {test} переводах ({tf} мошеннических), которых не видела: поймано {rec} мошенничества, {prec} тревог настоящие.",
    "Работает внутри Flink как файл ONNX: {model_ms} на перевод в среднем по последним решениям.",
    "Каждую тревогу объясняет: считает вклад признаков в риск и показывает аналитику главные причины словами."],
  rules_title: "Правила",
  rules_intro: "Правила — известные схемы мошенничества, записанные заранее. Решение принимает модель; два правила регулятор требует проверять всегда, остальные дают тревоге название или понятную аналитику причину.",
  r_rule: "Правило", r_checks: "Что проверяет", r_does: "Что делает",
  role_must: "сам отправляет на проверку — требует регулятор", role_name: "даёт название: {type}", role_reason: "причина для аналитика",
  rule_text: {
    NEW_PAYEE_HIGH_AMOUNT: "Первый перевод новому получателю: от {NEW_PAYEE_ABS_FLOOR} сум и больше чем в {NEW_PAYEE_AMOUNT_FACTOR} раза выше обычной суммы отправителя.",
    VELOCITY: "Больше {VELOCITY_MAX_COUNT} переводов за 10 минут.",
    STRUCTURING: "{STRUCTURING_MIN_COUNT} и больше переводов за час, каждый чуть ниже порога отчётности {STRUCTURING_THRESHOLD} сум.",
    DISTINCT_PAYEE_BURST: "Больше {DISTINCT_PAYEE_MAX} разных получателей за 10 минут.",
    GEO_ANOMALY: "Перевод не из того региона, откуда клиент обычно платит.",
    IMPOSSIBLE_TRAVEL: "Переезд больше чем на {MIN_TRAVEL_DISTANCE_KM} км быстрее {MAX_PLAUSIBLE_KMH} км/ч.",
    AMOUNT_DEVIATION: "Сумма дальше {AMOUNT_DEVIATION_SIGMA} стандартных отклонений от обычной для отправителя; нужно хотя бы {AMOUNT_DEVIATION_MIN_HISTORY} прошлых переводов.",
    COACHED_SESSION: "Во время подтверждения идёт звонок, и человек подтверждает дольше обычного больше чем на {COACHED_SESSION_Z} стандартных отклонения — похоже, им руководят по телефону.",
    DAILY_LIMIT_BREACH: "Переводы за день превысили дневной лимит {LIMIT_DAILY} сум.",
    MULE_FAN_IN: "{MULE_FAN_IN_MIN_SENDERS} и больше разных людей заплатили одному счёту за час."
  }
},
en: {
  intro: "The system checks every P2P card transfer in real time: hard rules and a machine-learning model judge it in a fraction of a second, and suspicious transfers go to an analyst. The system blocks nothing by itself — a person decides. Click any component to read about it.",
  flow_title: "What the system is made of",
  group: {in: "Taking the transfer in", engine: "Flink: the decision, in milliseconds", out: "After the decision", offline: "Offline, not in the live path"},
  parts: {
    gen: ["Transfer generator", "Python", "where the transfers come from", [
      "Our generator creates realistic P2P transfers on UzCard and HUMO cards: amounts, regions, times, people's habits.",
      "{rows} transfers over a month, {share} fraud, of four types: phone scam, account takeover, money mule, structuring.",
      "The data is generated because no real Uzbek transfer data is public.",
      "In a bank, the payment switch would stand here. The demo sends transfers from the part of the data the model never saw."]],
    raw: ["Kafka: the incoming queue", "Apache Kafka", "transactions.raw", [
      "A durable message queue: no transfer is lost, even while a worker restarts.",
      "The message key is the sender's card, so one person's transfers reach one worker, in order.",
      "Kafka stamps each transfer with its own write time, which times the stage 'into the incoming queue'."]],
    decode: ["Reading the message", "Flink", "the job's first step", [
      "The Flink job takes the transfer from the queue and unpacks it, decrypting it when it arrived encrypted.",
      "A broken record (no amount, a bad format) is dropped and counted rather than crashing the job."]],
    state: ["The sender's history", "Flink state", "a memory of every sender", [
      "Flink keeps each sender's history in its own memory: usual amounts, payees, transfer times, regions.",
      "That is where features like 'six times the usual amount' and 'a new payee' come from.",
      "The history survives a restart: Flink saves it to disk regularly."]],
    redis: ["The receiver's side", "Redis", "who pays this account", [
      "Redis keeps what one sender's history cannot show: how many different people paid an account in the last hour, day and week, and how much money came in.",
      "That is how a money mule is caught: many people pay one account, and the money moves straight on."]],
    rules: ["Features and rules", "Flink", "21 features, 10 rules", [
      "21 features: the sender's amounts and habits, whether the payee is new, how fast transfers come, money flowing into the payee, a call during confirmation and more.",
      "10 hard rules — known fraud patterns. In full in the Rules panel below."]],
    model: ["The model", "LightGBM · ONNX", "the risk score", [
      "Five gradient-boosted decision-tree models, their answers averaged.",
      "It runs inside Flink as an ONNX file, with no call to an outside service.",
      "In full in the Model panel below."]],
    decide: ["The decision", "Flink", "allow, or send to review", [
      "A transfer goes to review when the model's risk is past the alert level or a rule the regulator requires fired.",
      "The system blocks nothing by itself: a person decides on a suspicious transfer.",
      "The rules give the alert its name — for example, 'phone scam'."]],
    out: ["Kafka: decisions and alerts", "Apache Kafka", "transactions.scored, fraud.alerts", [
      "Every decision goes to one topic, and alerts to a second one as well.",
      "So writing to the database and the analyst's work do not hold each other up."]],
    sink: ["Sink writer", "Python", "sink-writer", [
      "Reads the decisions from Kafka and writes them to ClickHouse in batches, and the alerts to the Neo4j graph.",
      "When a database is down it keeps running and counts how many records were lost."]],
    ch: ["ClickHouse", "ClickHouse", "the warehouse", [
      "Every decision with the time of each stage — the Data & results tab is built from them.",
      "An audit chain: each record is linked to the one before by a hash, so the history cannot be changed unnoticed.",
      "The analyst's cases and verdicts."]],
    neo: ["Alert graph", "Neo4j", "who paid whom", [
      "Alerts as a graph: accounts are points, transfers are arrows.",
      "A mule ring shows as many arrows converging on one account."]],
    cases: ["Case manager", "Python", "case-manager", [
      "Opens a case for every alert, with its reasons in words — from each feature's share of the model's score.",
      "The analyst marks it fraud or a false alarm. Those marks are the only real labels the system gets; a bank would retrain the model on them."]],
    dash: ["Monitoring", "Grafana · demo", "what the bank watches", [
      "Grafana shows transfer and alert counts, types, the risk distribution, regions.",
      "This demo shows the same decisions live."]],
    train: ["Model training", "Python", "not in the live path", [
      "Runs the same feature code over the generator's data, so the features in training and in production are the same.",
      "Trains five LightGBM models, chooses the alert level on separate data and exports the model to ONNX.",
      "In full in the Model panel below."]]
  },
  close: "Close",
  walk_title: "One transfer's path",
  walk_hint: "Pick a real decision of the system and press Show the path: the transfer moves through the components, each step with its own time.",
  w_alert: "Latest alert", w_allow: "Ordinary transfer", w_selected: "Selected on the desk",
  w_play: "Show the path", w_pause: "Pause", w_prev: "◀ Back", w_next: "Next ▶",
  w_none: "No decisions yet: start the stream or show a fraud case on the desk.",
  w_ready: "Press Show the path, or Next.",
  m_stage: "this stage", m_elapsed: "since the transfer arrived",
  walk: {
    gen: ["The transfer is sent", "A transfer of {amount} UZS from card {from} to card {to}. It comes from our generator's data and is sent into the system."],
    raw: ["Into the incoming queue", "Kafka wrote the transfer to the incoming queue. The key is the sender's card, so one person's whole history reaches one worker."],
    handoff: ["Waiting to be handed on", "Flink takes transfers from the queue and passes them on in small batches. This wait is almost all of the decision time."],
    decode: ["Reading", "The transfer is unpacked and checked."],
    state: ["The sender's history", "This sender's history is read from Flink's memory: usual amounts, payees, the times of earlier transfers."],
    redis: ["The receiver's side", "Redis gives how many different people paid this account in the last hour, day and week."],
    rules: ["Features and rules", "21 features are computed and 10 rules checked. {rules}"],
    model: ["The model", "The model weighs all 21 features and scores the risk: {score}."],
    decide: ["The decision", "{decision}. {why}"],
    out: ["The decision leaves", "The decision goes to Kafka{alert}. From the transfer's arrival to the decision: {total}."],
    sink: ["Written down", "The sink writer takes the decisions in batches. The decision is already made, so nothing here needs to hurry."],
    ch: ["ClickHouse", "The database keeps the decision, the time of every stage and a record in the audit chain."],
    neo: ["The graph", "The alert becomes a link in the Neo4j graph: who paid whom."],
    neo_allow: ["The graph", "An ordinary transfer does not reach the graph: it holds alerts only."],
    cases: ["The analyst", "A case opens with its reasons in words{reason}. The analyst will mark it fraud or a false alarm."],
    cases_allow: ["The analyst", "There is no alert, so the analyst never sees this transfer."],
    dash: ["Monitoring", "Grafana and this demo show the whole flow: transfers, alerts, types and times."]
  },
  fired: "Fired: {list}.", none_fired: "No rule fired.",
  why_must: "A rule the regulator requires fired, and such a transfer always goes to review.",
  why_review: "The risk {score} is past the alert level {cut}.",
  why_allow: "The risk {score} is under the alert level {cut}, and no rule asked for a review.",
  alert_too: ", and the alert to the alert queue as well",
  reason: "; the main one: '{label}'",
  model_title: "The model",
  model_lines: [
    "Five LightGBM models — gradient boosting on decision trees; their answers are averaged.",
    "It reads 21 features: the sender's amounts and habits, whether the payee is new, how fast transfers come, money flowing into the payee, a call during confirmation.",
    "Trained on the generator's first {fit} transfers in time. The alert level {cut} was chosen on the next {val}.",
    "Tested on the last {test} transfers ({tf} of them fraud), which it never saw: it caught {rec} of the fraud, and {prec} of its alerts were real.",
    "It runs inside Flink as an ONNX file: {model_ms} per transfer on average over the latest decisions.",
    "It explains every alert: each feature's share of the risk, with the main reasons shown to the analyst in words."],
  rules_title: "The rules",
  rules_intro: "The rules are known fraud patterns, written down in advance. The model makes the decision; the regulator requires two rules to be reviewed every time, and the others give the alert a name or the analyst a readable reason.",
  r_rule: "Rule", r_checks: "What it checks", r_does: "What it does",
  role_must: "sends to review by itself — the regulator requires it", role_name: "names the alert: {type}", role_reason: "a reason for the analyst",
  rule_text: {
    NEW_PAYEE_HIGH_AMOUNT: "A first transfer to a new payee: {NEW_PAYEE_ABS_FLOOR} UZS or more, and more than {NEW_PAYEE_AMOUNT_FACTOR} times the sender's usual amount.",
    VELOCITY: "More than {VELOCITY_MAX_COUNT} transfers in 10 minutes.",
    STRUCTURING: "{STRUCTURING_MIN_COUNT} or more transfers in an hour, each just under the {STRUCTURING_THRESHOLD} UZS reporting threshold.",
    DISTINCT_PAYEE_BURST: "More than {DISTINCT_PAYEE_MAX} different payees in 10 minutes.",
    GEO_ANOMALY: "A transfer from outside the region the customer usually pays from.",
    IMPOSSIBLE_TRAVEL: "A move of more than {MIN_TRAVEL_DISTANCE_KM} km faster than {MAX_PLAUSIBLE_KMH} km/h.",
    AMOUNT_DEVIATION: "An amount more than {AMOUNT_DEVIATION_SIGMA} standard deviations from the sender's usual, with at least {AMOUNT_DEVIATION_MIN_HISTORY} earlier transfers.",
    COACHED_SESSION: "A call is in progress while the person confirms, and they take more than {COACHED_SESSION_Z} standard deviations longer than usual — someone seems to be guiding them by phone.",
    DAILY_LIMIT_BREACH: "The day's transfers passed the {LIMIT_DAILY} UZS daily limit.",
    MULE_FAN_IN: "{MULE_FAN_IN_MIN_SENDERS} or more different people paid one account within an hour."
  }
}};

const ab = k => (ABOUT[lang][k] ?? ABOUT.en[k] ?? k);
const walk = {pool: [], rec: null, choice: "alert", idx: -1, timer: null, timing: null};

async function renderAbout() {
  drawAbout();
  try {
    const [s, live] = await Promise.all([api("/api/stream?limit=400"), api("/api/live")]);
    walk.pool = s.rows || [];
    walk.timing = live.timing || null;
  } catch (e) { /* the gate screen covers a stack that stopped answering */ }
  if (!walk.rec) walk.rec = candidates()[walk.choice] || null;
  drawAbout();
}

function candidates() {
  return {
    alert: walk.pool.find(r => r.decision === "REVIEW"),
    allow: walk.pool.find(r => r.decision === "ALLOW"),
    selected: selected && selected.src === "stream" ? selected.row : null
  };
}

function drawAbout() {
  const o = (results && results.own) || {};
  const byGroup = g => COMPONENTS.filter(c => c.group === g).map(c => {
    const [name, kind, sub] = ab("parts")[c.id];
    return `<button type="button" class="comp ${c.store ? "store" : ""} ${c.ml ? "ml" : ""}" data-comp="${c.id}">` +
      `<span class="kind">${esc(kind)}</span><span class="name">${esc(name)}</span><span class="sub">${esc(sub)}</span></button>`;
  }).join("");
  const modelMs = (((walk.timing || {}).stages || []).find(s => s.name === "model") || {}).ms;
  const cut = status && status.cut;
  $("#about").innerHTML = `
    <p class="about-intro">${esc(ab("intro"))}</p>
    <div class="panel">
      <div class="panel-h"><h2>${esc(ab("flow_title"))}</h2></div>
      <div class="flow" id="flow">
        ${GROUPS.map(g => `<div class="flow-group"><h3>${esc(ab("group")[g])}</h3><div class="flow-row">${byGroup(g)}</div></div>`).join("")}
        <span id="packet" hidden></span>
      </div>
      <div class="walk">
        <div><h4>${esc(ab("walk_title"))}</h4><p class="small-note">${esc(ab("walk_hint"))}</p></div>
        <div class="walk-controls">
          ${["alert", "allow", "selected"].map(k => `<button type="button" data-walk="${k}" aria-pressed="${walk.choice === k}"` +
            `${candidates()[k] ? "" : " disabled"}>${esc(ab("w_" + k))}</button>`).join("")}
        </div>
        <div class="walk-controls">
          <button type="button" class="primary" data-walk-play ${walk.rec ? "" : "disabled"}>${esc(ab(walk.timer ? "w_pause" : "w_play"))}</button>
          <button type="button" data-walk-step="-1" ${walk.rec ? "" : "disabled"}>${esc(ab("w_prev"))}</button>
          <button type="button" data-walk-step="1" ${walk.rec ? "" : "disabled"}>${esc(ab("w_next"))}</button>
        </div>
        <div id="walk-step"></div>
      </div>
    </div>
    <div class="about-grid">
      <div class="panel">
        <div class="panel-h"><h2>${esc(ab("model_title"))}</h2></div>
        <ul class="model-list">${ab("model_lines").map(line => `<li>${esc(fill(line, {
          fit: fmtInt(o.fit), val: fmtInt(o.val), test: fmtInt(o.test), tf: fmtInt(o.test_fraud),
          rec: pct(o.recall), prec: pct(o.precision), cut: dec(cut, 4), model_ms: fmtMs(modelMs)}))}</li>`).join("")}</ul>
      </div>
      <div class="panel">
        <div class="panel-h"><h2>${esc(ab("rules_title"))}</h2></div>
        <p class="panel-sub">${esc(ab("rules_intro"))}</p>
        <div class="tbl"><table class="rules-table">
          <thead><tr><th>${esc(ab("r_rule"))}</th><th>${esc(ab("r_checks"))}</th><th>${esc(ab("r_does"))}</th></tr></thead>
          <tbody>${Object.keys(RULE_FACTS).map(code => `<tr><td>${esc(t("rules")[code] || code)}</td>` +
            `<td>${esc(ruleText(code))}</td><td>${roles(code)}</td></tr>`).join("")}</tbody>
        </table></div>
      </div>
    </div>
    <div class="modal" id="modal" hidden><div class="modal-card" role="dialog" aria-modal="true" id="modal-card"></div></div>`;
  paintWalk();
}

function ruleText(code) {
  const facts = Object.fromEntries(Object.entries(RULE_FACTS[code])
    .map(([k, v]) => [k, v >= 1000 ? fmtInt(v) : String(v)]));
  return fill(ab("rule_text")[code], facts);
}

function roles(code) {
  const named = NAMED_BY.find(([, rules]) => rules.includes(code));
  const out = [];
  if (MUST.has(code)) out.push(`<span class="role must">${esc(ab("role_must"))}</span>`);
  if (named) out.push(`<span class="role name">${esc(fill(ab("role_name"), {type: kindName(named[0]).toLowerCase()}))}</span>`);
  if (!out.length) out.push(`<span class="role reason">${esc(ab("role_reason"))}</span>`);
  return out.join(" ");
}

function openComponent(id) {
  const [name, kind, , lines] = ab("parts")[id];
  const o = (results && results.own) || {};
  const vals = {rows: fmtInt(o.rows), share: o.fraud_share == null ? "—" : dec(o.fraud_share * 100, 2) + "%"};
  $("#modal-card").innerHTML = `<button type="button" class="modal-close" data-close>${esc(ab("close"))}</button>` +
    `<span class="kind">${esc(kind)}</span><h3>${esc(name)}</h3>` +
    `<ul>${lines.map(line => `<li>${esc(fill(line, vals))}</li>`).join("")}</ul>`;
  $("#modal").hidden = false;
  $("#modal-card .modal-close").focus();
}

function closeComponent() { const m = $("#modal"); if (m) m.hidden = true; }

// Each step's own time, and the time since arrival once the step is done.
function stepTimes(i) {
  const st = walk.rec.stages || {};
  let since = 0;
  for (let k = 0; k <= i; k++) since += Number(st[WALK[k].stage]) || 0;
  const s = WALK[i].stage;
  return {own: s && st[s] != null ? Number(st[s]) : null, since};
}

function stepText(i) {
  const r = walk.rec, step = WALK[i], alert = r.decision === "REVIEW";
  const key = step.alertOnly && !alert ? step.at + "_allow" : (step.stage === "handoff" ? "handoff" : step.at);
  const [title, text] = ab("walk")[key];
  const rules = r.rules || [], cut = status && status.cut;
  const top = r.why && r.why.items && r.why.items[0];
  const label = top ? (lang === "ru" ? (T.ru.feat[top.feature] || top.label_en) : top.label_en) : "";
  return [title, fill(text, {
    amount: fmtInt(r.amount), from: r.from, to: r.to,
    rules: rules.length ? fill(ab("fired"), {list: rules.map(x => t("rules")[x] || x).join(", ")}) : ab("none_fired"),
    score: dec(r.score, 2), decision: t("dec")[r.decision] || r.decision,
    why: rules.some(x => MUST.has(x)) ? ab("why_must")
      : fill(ab(alert ? "why_review" : "why_allow"), {score: dec(r.score, 2), cut: dec(cut, 2)}),
    alert: alert ? ab("alert_too") : "", total: fmtMs(r.ms),
    reason: label ? fill(ab("reason"), {label}) : ""})];
}

function paintWalk() {
  const box = $("#walk-step");
  if (!box) return;
  document.querySelectorAll("#flow .comp").forEach(el => el.classList.remove("on", "past"));
  const packet = $("#packet");
  if (!walk.rec) { box.innerHTML = `<p class="small-note">${esc(ab("w_none"))}</p>`; packet.hidden = true; return; }
  const r = walk.rec;
  const head = `<div class="walk-meta"><span><b>${fmtInt(r.amount)} ${t("sum")}</b></span>` +
    `<span>${esc(last4(r.from))} → ${esc(last4(r.to))}</span><span>${badge(r.decision)}</span><span>${esc(fmtTime(r.at))}</span></div>`;
  if (walk.idx < 0) { box.innerHTML = head + `<p class="small-note">${esc(ab("w_ready"))}</p>`; packet.hidden = true; return; }
  const [title, text] = stepText(walk.idx);
  const times = stepTimes(walk.idx);
  box.innerHTML = head + `<h4>${esc(title)}</h4><p>${esc(text)}</p>` +
    `<div class="walk-meta">${times.own == null ? "" : `<span>${esc(ab("m_stage"))}: <b>${fmtMs(times.own)}</b></span>`}` +
    `<span>${esc(ab("m_elapsed"))}: <b>${fmtMs(times.since)}</b></span></div>`;
  WALK.forEach((s, k) => {
    const el = document.querySelector(`#flow [data-comp="${s.at}"]`);
    if (el && k < walk.idx) el.classList.add("past");
  });
  const at = document.querySelector(`#flow [data-comp="${WALK[walk.idx].at}"]`);
  at.classList.remove("past");
  at.classList.add("on");
  const flow = $("#flow").getBoundingClientRect(), b = at.getBoundingClientRect();
  packet.hidden = false;
  packet.classList.toggle("alert", r.decision === "REVIEW" && walk.idx >= WALK.findIndex(s => s.at === "decide"));
  packet.style.transform = `translate(${b.left - flow.left + b.width / 2 - 8}px, ${b.top - flow.top - 8}px)`;
}

function walkStop() {
  clearInterval(walk.timer);
  walk.timer = null;
  const btn = document.querySelector("[data-walk-play]");
  if (btn) btn.textContent = ab("w_play");
}

function walkGo(delta) {
  walk.idx = Math.max(0, Math.min(WALK.length - 1, walk.idx + delta));
  paintWalk();
}

function walkPlay() {
  if (walk.timer) { walkStop(); return; }
  if (walk.idx >= WALK.length - 1) walk.idx = -1;
  walkGo(1);
  walk.timer = setInterval(() => (walk.idx >= WALK.length - 1 ? walkStop() : walkGo(1)), 2200);
  document.querySelector("[data-walk-play]").textContent = ab("w_pause");
}

document.addEventListener("click", e => {
  const el = e.target.closest("[data-comp],[data-walk],[data-walk-play],[data-walk-step],[data-close],#modal");
  if (!el) return;
  if (el.dataset.comp) openComponent(el.dataset.comp);
  else if (el.dataset.walk) {
    walkStop();
    walk.choice = el.dataset.walk;
    walk.rec = candidates()[walk.choice] || null;
    walk.idx = -1;
    drawAbout();
  }
  else if (el.hasAttribute("data-walk-play")) walkPlay();
  else if (el.dataset.walkStep) { walkStop(); walkGo(Number(el.dataset.walkStep)); }
  else if (el.hasAttribute("data-close") || e.target === el) closeComponent();
});
document.addEventListener("keydown", e => { if (e.key === "Escape") closeComponent(); });
window.addEventListener("resize", () => { if (walk.idx >= 0) paintWalk(); });

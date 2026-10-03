/* The "About the project" tab: what the system is made of (a click on a part opens
   its details), the path one real decision of the running system took through it,
   with that decision's own stage times, and the model, its features and the rules.
   Nothing here is sample data. Loaded after index.html's script, whose helpers it
   uses (t, esc, fill, dec, pct, fmtInt, fmtMs, fmtTime, badge, kindName, last4, api,
   openModal, lang, status, results, selected, MUST, T). */

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
// The model's 24 features in ml/models/feature_names.json's order, grouped as
// stream-processor/capabilities.py groups them (demo/tests pins the list).
const FEATURE_GROUPS = [
  ["sender", ["log_amount", "amount_to_mean", "amount_z", "is_new_payee", "vel_10m", "vel_1h",
              "distinct_payees_10m", "sub_threshold_1h", "secs_since_last", "daily_sum_ratio", "hour"]],
  ["receiver", ["rcv_distinct_senders_1h", "rcv_inflow_1h"]],
  ["place", ["geo_is_anomaly"]],
  ["session", ["active_call", "secs_login_z"]],
  ["links", ["payee_payers_24h", "payee_payers_7d", "sender_payees_24h", "sender_payees_7d",
             "secs_since_sender_inbound"]],
  ["confirmed", ["payee_flagged", "sender_flagged", "payee_flagged_contacts"]]
];

const GROUPS = ["in", "engine", "out"];
const COMPONENTS = [
  {id: "gen", group: "in"}, {id: "raw", group: "in"},
  {id: "decode", group: "engine"}, {id: "state", group: "engine"}, {id: "redis", group: "engine", store: true},
  {id: "rules", group: "engine"}, {id: "model", group: "engine", ml: true}, {id: "decide", group: "engine"},
  {id: "out", group: "out"}, {id: "second", group: "out", ml: true},
  {id: "sink", group: "out"}, {id: "ch", group: "out", store: true},
  {id: "cases", group: "out"}, {id: "dash", group: "out"}
];
// The path one transfer takes: the component it is at, and the stage of the
// decision's stage_ms that times it.
const WALK = [
  {key: "gen", at: "gen"}, {key: "raw", at: "raw", stage: "kafka"},
  {key: "handoff", at: "decode", stage: "handoff"}, {key: "decode", at: "decode", stage: "decode"},
  {key: "state", at: "state", stage: "state"}, {key: "redis", at: "redis", stage: "redis"},
  {key: "rules", at: "rules", stage: "rules"}, {key: "model", at: "model", stage: "model"},
  {key: "decide", at: "decide", stage: "decide"}, {key: "stored", at: "ch"}, {key: "analyst", at: "cases"}
];

const ABOUT = {
ru: {
  intro: "Система проверяет каждый P2P-перевод по картам в реальном времени: жёсткие правила и модель машинного обучения оценивают его за доли секунды. Если риск чуть ниже уровня тревоги, перевод за секунду-две перепроверяет вторая модель, TabPFN. Подозрительные переводы задерживаются до решения аналитика: окончательно блокирует или отпускает перевод человек.",
  flow_title: "Из чего состоит система",
  legend: "Нажмите на карточку, чтобы прочитать о компоненте. Серые карточки — хранилища данных; синяя рамка — модель машинного обучения.",
  group: {in: "Приём перевода", engine: "Flink: решение за миллисекунды", out: "После решения"},
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
      "Так ловится дроп-счёт: много людей платят одному счёту, и деньги сразу уходят дальше.",
      "Ещё Redis хранит список карт из подтверждённых мошенничеств. Его начало — прошлые случаи, а пополняет его аналитик, когда блокирует перевод."]],
    rules: ["Признаки и правила", "Flink", "24 признака, 10 правил", [
      "24 признака: суммы и привычки отправителя, новизна получателя, скорость переводов, приток денег получателю, звонок во время подтверждения, связь с подтверждёнными мошенниками и другие.",
      "10 жёстких правил — известные схемы мошенничества. Полные списки — по кнопкам в блоках «Модель» и «Правила» ниже."]],
    model: ["Модель", "LightGBM · ONNX", "оценка риска", [
      "Пять моделей градиентного бустинга на деревьях решений, их ответы усредняются.",
      "Работает внутри Flink как файл ONNX, без обращения к внешнему сервису.",
      "Подробно — в блоке «Модель» ниже."]],
    decide: ["Решение", "Flink", "пропустить, задержать или перепроверить", [
      "Перевод задерживается, если риск модели выше уровня тревоги или сработало обязательное правило регулятора.",
      "Если риск чуть ниже уровня тревоги, перевод уходит на вторую проверку.",
      "Система сама ничего не блокирует: подозрительный перевод решает человек.",
      "Правила дают тревоге название — например, «мошенник по телефону»."]],
    out: ["Kafka: решения и тревоги", "Apache Kafka", "transactions.scored, fraud.alerts, fraud.second_look", [
      "Каждое решение уходит в одну очередь, тревоги — ещё и в отдельную, а переводы для второй проверки — в третью.",
      "Так запись в базу и работа аналитика не мешают друг другу."]],
    second: ["Вторая проверка", "Python · TabPFN", "second-look", [
      "Переводы с риском чуть ниже уровня тревоги ждут около секунды, пока их проверит вторая модель — TabPFN.",
      "Видит мошенничество — перевод задерживается и попадает к аналитику, иначе уходит.",
      "Не ответила за 5 секунд или недоступна — перевод не ждёт дальше: он задерживается для аналитика.",
      "На отложенных данных в этой полосе было 20 мошенничеств, которые основная модель пропускала. Вторая проверка нашла 14 из них, и доля пойманного мошенничества выросла с 70,5% до 78,4%."]],
    sink: ["Запись в базу", "Python", "sink-writer", [
      "Читает решения из Kafka и пачками записывает их в ClickHouse.",
      "Если база недоступна, продолжает работать и считает, сколько записей потеряно."]],
    ch: ["ClickHouse", "ClickHouse", "хранилище", [
      "Каждое решение со временем всех этапов — по ним строится вкладка «Данные и результаты».",
      "Цепочка аудита: каждая запись связана с предыдущей хешем, поэтому незаметно изменить историю нельзя.",
      "Дела аналитика и его отметки."]],
    cases: ["Разбор тревог", "Python", "case-manager", [
      "На каждую тревогу открывает дело с причинами словами — из вклада признаков в оценку модели.",
      "Аналитик решает: заблокировать перевод (мошенничество) или отпустить (ложная тревога). Эти решения — единственные настоящие метки в системе, на них банк переобучал бы модель.",
      "Заблокированный перевод система запоминает сразу: карта получателя попадает в список подтверждённых мошенников, и модель видит это на следующих переводах с этой карты и на неё.",
      "Дела можно отобрать по тому, кто задержал перевод: основная модель, жёсткое правило или вторая проверка."]],
    dash: ["Мониторинг", "Grafana · демо", "что видит банк", [
      "Grafana показывает число переводов и тревог, типы, распределение риска, регионы.",
      "Это демо показывает те же решения вживую."]]
  },
  walk_title: "Путь настоящего перевода",
  walk_hint: "Берём перевод, который система только что обработала, и показываем, через что он прошёл и сколько занял каждый шаг. Нажмите «Проиграть путь» или на любой шаг.",
  w_alert: "Последняя тревога", w_allow: "Последний обычный перевод", w_selected: "Выбранный на рабочем месте",
  w_play: "▶ Проиграть путь", w_pause: "Пауза",
  w_none: "Пока нет решений: запустите поток или покажите случай мошенничества на рабочем месте.",
  c_risk: "Риск модели", c_level: "уровень тревоги {cut}", c_rules: "Правила", c_named: "Название тревоги",
  c_reason: "Главная причина", c_total: "От прихода до решения", c_none: "не сработали",
  walk: {
    gen: ["Перевод отправлен", "Перевод взят из данных нашего генератора и отправлен в систему, как его отправил бы банк."],
    raw: ["Записан в очередь", "Kafka записала перевод во входную очередь. Ключ — карта отправителя, поэтому вся история одного человека попадает к одному обработчику."],
    handoff: ["Ждёт передачи", "Flink забирает переводы из очереди и передаёт на обработку небольшими пачками. Это ожидание — почти всё время решения."],
    decode: ["Прочитан", "Перевод распакован и проверен."],
    state: ["История отправителя", "Из памяти Flink прочитана история этого отправителя: обычные суммы, получатели, время прошлых переводов."],
    redis: ["Сторона получателя", "Из Redis прочитано, сколько разных людей платили этому счёту за час, сутки и неделю, и нет ли карт этого перевода в списке подтверждённых мошенников."],
    rules: ["Признаки и правила", "Посчитано 24 признака и проверены 10 правил. {rules}"],
    model: ["Модель", "Модель взвесила все 24 признака и оценила риск: {score}."],
    decide: ["Решение", "{decision}. {why}"],
    stored: ["Записан в базу", "Решение ушло в Kafka{alert} и записано в ClickHouse вместе со временем каждого этапа."],
    analyst: ["У аналитика", "Открыто дело с причинами словами. Перевод задержан, пока аналитик его не заблокирует или не отпустит."],
    analyst_allow: ["Аналитик не нужен", "Тревоги нет, поэтому перевод прошёл, а аналитик его не увидит."]
  },
  fired: "Сработали: {list}.", none_fired: "Ни одно правило не сработало.",
  why_must: "Сработало обязательное правило регулятора — такой перевод всегда задерживается для аналитика.",
  why_review: "Риск {score} выше уровня тревоги {cut}.",
  why_allow: "Риск {score} ниже уровня тревоги {cut}, и ни одно правило не требует проверки.",
  alert_too: ", а тревога — ещё и в очередь тревог",
  model_title: "Модель",
  model_lines: [
    "Пять моделей LightGBM — градиентный бустинг на деревьях решений; их ответы усредняются.",
    "Смотрит на 24 признака перевода — список по кнопке ниже.",
    "Обучена на первых {fit} переводах генератора по времени. Уровень тревоги {cut} подобран на следующих {val}.",
    "Проверена на последних {test} переводах ({tf} мошеннических), которых не видела: поймано {rec} мошенничества, {prec} тревог настоящие.",
    "Работает внутри Flink как файл ONNX: {model_ms} на перевод в среднем по последним решениям.",
    "Каждую тревогу объясняет: считает вклад признаков в риск и показывает аналитику главные причины словами."],
  features_btn: "Список признаков ({n})",
  features_title: "Признаки модели",
  features_intro: "Задача Flink считает их для каждого перевода; модель смотрит на все сразу.",
  fgroup: {sender: "Отправитель и сумма", receiver: "Получатель за последний час", place: "Место",
           session: "Сессия в приложении", links: "Связи за сутки и неделю",
           confirmed: "Подтверждённые мошенники"},
  feature: {
    log_amount: "Сумма перевода.",
    amount_to_mean: "Во сколько раз сумма больше обычной суммы этого отправителя.",
    amount_z: "Насколько сумма выбивается из обычного разброса сумм отправителя.",
    is_new_payee: "Платит ли отправитель этому получателю впервые.",
    vel_10m: "Сколько переводов отправитель сделал за последние 10 минут.",
    vel_1h: "Сколько переводов он сделал за последний час.",
    distinct_payees_10m: "Скольким разным получателям он заплатил за 10 минут.",
    sub_threshold_1h: "Сколько его переводов за час чуть ниже порога отчётности 10 млн сум.",
    secs_since_last: "Сколько времени прошло с его прошлого перевода.",
    daily_sum_ratio: "Какую долю дневного лимита он уже потратил.",
    hour: "Час суток.",
    rcv_distinct_senders_1h: "Сколько разных людей заплатили получателю за час.",
    rcv_inflow_1h: "Сколько денег пришло получателю за час.",
    geo_is_anomaly: "Перевод не из обычного региона отправителя.",
    active_call: "Шёл ли телефонный звонок, пока человек подтверждал перевод.",
    secs_login_z: "Насколько дольше обычного человек подтверждал перевод.",
    payee_payers_24h: "Сколько разных людей платили получателю за сутки.",
    payee_payers_7d: "Сколько разных людей платили получателю за неделю.",
    sender_payees_24h: "Скольким разным получателям платил отправитель за сутки.",
    sender_payees_7d: "Скольким разным получателям платил отправитель за неделю.",
    secs_since_sender_inbound: "Сколько времени прошло с тех пор, как отправителю самому заплатили: дроп-счёт быстро отправляет полученное дальше.",
    payee_flagged: "Получатель уже принимал деньги в подтверждённом мошенничестве.",
    sender_flagged: "Отправитель сам раньше принимал деньги в подтверждённом мошенничестве: дроп пересылает их дальше.",
    payee_flagged_contacts: "Сколько карт, с которыми получатель имел дело за неделю, принимали деньги в подтверждённом мошенничестве."
  },
  rules_title: "Правила",
  rules_intro: "Правила — известные схемы мошенничества, записанные заранее. Решение принимает модель; два правила регулятор требует проверять всегда, остальные дают тревоге название или понятную аналитику причину.",
  rules_btn: "Список правил ({n})",
  r_rule: "Правило", r_checks: "Что проверяет", r_does: "Что делает",
  role_must: "сам задерживает перевод — требует регулятор", role_name: "даёт название: {type}", role_reason: "причина для аналитика",
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
  intro: "The system checks every P2P card transfer in real time: hard rules and a machine-learning model judge it in a fraction of a second. When the risk is just under the alert level, a second model, TabPFN, takes another look within a second or two. Suspicious transfers are held until an analyst decides: a person blocks or releases them.",
  flow_title: "What the system is made of",
  legend: "Click a card to read about the component. Grey cards hold data; the blue frame is the machine-learning model.",
  group: {in: "Taking the transfer in", engine: "Flink: the decision, in milliseconds", out: "After the decision"},
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
      "That is how a money mule is caught: many people pay one account, and the money moves straight on.",
      "Redis also keeps the list of cards from confirmed frauds. It starts from past cases, and the analyst adds to it on blocking a transfer."]],
    rules: ["Features and rules", "Flink", "24 features, 10 rules", [
      "24 features: the sender's amounts and habits, whether the payee is new, how fast transfers come, money flowing into the payee, a call during confirmation, links to confirmed fraudsters and more.",
      "10 hard rules — known fraud patterns. The full lists open from the Model and Rules panels below."]],
    model: ["The model", "LightGBM · ONNX", "the risk score", [
      "Five gradient-boosted decision-tree models, their answers averaged.",
      "It runs inside Flink as an ONNX file, with no call to an outside service.",
      "In full in the Model panel below."]],
    decide: ["The decision", "Flink", "allow, hold or look again", [
      "A transfer is held when the model's risk is past the alert level or a rule the regulator requires fired.",
      "When the risk is just under the alert level, the transfer goes to the second look.",
      "The system blocks nothing by itself: a person decides on a suspicious transfer.",
      "The rules give the alert its name — for example, 'phone scam'."]],
    out: ["Kafka: decisions and alerts", "Apache Kafka", "transactions.scored, fraud.alerts, fraud.second_look", [
      "Every decision goes to one topic, alerts to a second one as well, and transfers for the second look to a third.",
      "So writing to the database and the analyst's work do not hold each other up."]],
    second: ["Second look", "Python · TabPFN", "second-look", [
      "Transfers with a risk just under the alert level wait about a second while a second model, TabPFN, looks at them.",
      "If it sees fraud, the transfer is held for the analyst; otherwise it goes.",
      "If it has not answered within 5 seconds, or is down, the transfer waits no longer: it is held for the analyst.",
      "On held-out data this band held 20 frauds the main model missed. The second look found 14 of them, and the share of fraud caught rose from 70.5% to 78.4%."]],
    sink: ["Sink writer", "Python", "sink-writer", [
      "Reads the decisions from Kafka and writes them to ClickHouse in batches.",
      "When a database is down it keeps running and counts how many records were lost."]],
    ch: ["ClickHouse", "ClickHouse", "the warehouse", [
      "Every decision with the time of each stage — the Data & results tab is built from them.",
      "An audit chain: each record is linked to the one before by a hash, so the history cannot be changed unnoticed.",
      "The analyst's cases and verdicts."]],
    cases: ["Case manager", "Python", "case-manager", [
      "Opens a case for every alert, with its reasons in words — from each feature's share of the model's score.",
      "The analyst blocks the transfer (fraud) or releases it (false alarm). Those verdicts are the only real labels the system gets; a bank would retrain the model on them.",
      "A blocked transfer teaches the system at once: the payee's card joins the list of confirmed fraud accounts, and the model sees it on the next transfers from or to that card.",
      "Cases can be filtered by what held the transfer: the model, a hard rule or the second look."]],
    dash: ["Monitoring", "Grafana · demo", "what the bank watches", [
      "Grafana shows transfer and alert counts, types, the risk distribution, regions.",
      "This demo shows the same decisions live."]]
  },
  walk_title: "The path of a real transfer",
  walk_hint: "We take a transfer the system has just handled and show what it passed through and how long each step took. Press Play the path, or click any step.",
  w_alert: "Latest alert", w_allow: "Latest ordinary transfer", w_selected: "Selected on the desk",
  w_play: "▶ Play the path", w_pause: "Pause",
  w_none: "No decisions yet: start the stream or show a fraud case on the desk.",
  c_risk: "Model risk", c_level: "alert level {cut}", c_rules: "Rules", c_named: "Alert name",
  c_reason: "Main reason", c_total: "From arrival to decision", c_none: "none fired",
  walk: {
    gen: ["Sent", "The transfer comes from our generator's data and is sent into the system as a bank would send it."],
    raw: ["Queued", "Kafka wrote the transfer to the incoming queue. The key is the sender's card, so one person's whole history reaches one worker."],
    handoff: ["Waiting to be handed on", "Flink takes transfers from the queue and passes them on in small batches. This wait is almost all of the decision time."],
    decode: ["Read", "The transfer is unpacked and checked."],
    state: ["The sender's history", "This sender's history is read from Flink's memory: usual amounts, payees, the times of earlier transfers."],
    redis: ["The receiver's side", "Redis gives how many different people paid this account in the last hour, day and week, and whether this transfer's cards are on the list of confirmed fraud accounts."],
    rules: ["Features and rules", "24 features are computed and 10 rules checked. {rules}"],
    model: ["The model", "The model weighs all 24 features and scores the risk: {score}."],
    decide: ["The decision", "{decision}. {why}"],
    stored: ["Written down", "The decision went to Kafka{alert} and was written to ClickHouse with the time of every stage."],
    analyst: ["With the analyst", "A case opened with its reasons in words. The transfer is held until the analyst blocks or releases it."],
    analyst_allow: ["No analyst needed", "There is no alert, so the transfer went through and no analyst sees it."]
  },
  fired: "Fired: {list}.", none_fired: "No rule fired.",
  why_must: "A rule the regulator requires fired, and such a transfer is always held for the analyst.",
  why_review: "The risk {score} is past the alert level {cut}.",
  why_allow: "The risk {score} is under the alert level {cut}, and no rule asked for a review.",
  alert_too: ", and the alert to the alert queue as well",
  model_title: "The model",
  model_lines: [
    "Five LightGBM models — gradient boosting on decision trees; their answers are averaged.",
    "It reads 24 features of the transfer — the list opens from the button below.",
    "Trained on the generator's first {fit} transfers in time. The alert level {cut} was chosen on the next {val}.",
    "Tested on the last {test} transfers ({tf} of them fraud), which it never saw: it caught {rec} of the fraud, and {prec} of its alerts were real.",
    "It runs inside Flink as an ONNX file: {model_ms} per transfer on average over the latest decisions.",
    "It explains every alert: each feature's share of the risk, with the main reasons shown to the analyst in words."],
  features_btn: "List of features ({n})",
  features_title: "The model's features",
  features_intro: "The Flink job computes them for every transfer; the model reads all of them at once.",
  fgroup: {sender: "The sender and the amount", receiver: "The payee in the last hour", place: "Place",
           session: "The app session", links: "Links over a day and a week",
           confirmed: "Confirmed fraud accounts"},
  feature: {
    log_amount: "The amount.",
    amount_to_mean: "How many times the sender's usual amount this is.",
    amount_z: "How far the amount falls outside the sender's usual spread.",
    is_new_payee: "Whether the sender pays this payee for the first time.",
    vel_10m: "How many transfers the sender made in the last 10 minutes.",
    vel_1h: "How many they made in the last hour.",
    distinct_payees_10m: "How many different payees they paid in 10 minutes.",
    sub_threshold_1h: "How many of their transfers in an hour sat just under the 10 million UZS reporting threshold.",
    secs_since_last: "How long since their previous transfer.",
    daily_sum_ratio: "How much of the daily limit they have used.",
    hour: "The hour of the day.",
    rcv_distinct_senders_1h: "How many different people paid the payee in the hour.",
    rcv_inflow_1h: "How much money reached the payee in the hour.",
    geo_is_anomaly: "The transfer comes from outside the sender's usual region.",
    active_call: "Whether a phone call was active while the person confirmed.",
    secs_login_z: "How much longer than usual the person took to confirm.",
    payee_payers_24h: "How many different people paid the payee in a day.",
    payee_payers_7d: "How many different people paid the payee in a week.",
    sender_payees_24h: "How many different payees the sender paid in a day.",
    sender_payees_7d: "How many different payees the sender paid in a week.",
    secs_since_sender_inbound: "How long since the sender was last paid: a mule forwards what it receives quickly.",
    payee_flagged: "The payee already received money in a confirmed fraud.",
    sender_flagged: "The sender itself received money in a confirmed fraud before: a mule passing it on.",
    payee_flagged_contacts: "How many of the cards the payee dealt with over the week received money in a confirmed fraud."
  },
  rules_title: "The rules",
  rules_intro: "The rules are known fraud patterns, written down in advance. The model makes the decision; the regulator requires two rules to be reviewed every time, and the others give the alert a name or the analyst a readable reason.",
  rules_btn: "List of rules ({n})",
  r_rule: "Rule", r_checks: "What it checks", r_does: "What it does",
  role_must: "holds the transfer by itself — the regulator requires it", role_name: "names the alert: {type}", role_reason: "a reason for the analyst",
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

// The walk follows the job's own decision: a transfer the second look decided took
// another path, and its stage times stayed with the job's record.
const byJob = r => r && !r.second && r.decision !== "SECOND_LOOK";

function candidates() {
  return {
    alert: walk.pool.find(r => r.decision === "REVIEW" && byJob(r)),
    allow: walk.pool.find(r => r.decision === "ALLOW" && byJob(r)),
    selected: selected && selected.src === "stream" && byJob(selected.row) ? selected.row : null
  };
}

function drawAbout() {
  const o = (results && results.own) || {};
  const card = c => {
    const [name, kind, sub] = ab("parts")[c.id];
    return `<button type="button" class="comp ${c.store ? "store" : ""} ${c.ml ? "ml" : ""}" data-comp="${c.id}">` +
      `<span class="kind">${esc(kind)}</span><span class="name">${esc(name)}</span><span class="sub">${esc(sub)}</span></button>`;
  };
  const modelMs = (((walk.timing || {}).stages || []).find(s => s.name === "model") || {}).ms;
  const features = FEATURE_GROUPS.reduce((n, [, names]) => n + names.length, 0);
  const cut = status && status.cut;
  $("#about").innerHTML = `
    <p class="about-intro">${esc(ab("intro"))}</p>
    <div class="panel">
      <div class="panel-h"><h2>${esc(ab("flow_title"))}</h2></div>
      <p class="panel-sub">${esc(ab("legend"))}</p>
      <div class="flow">${GROUPS.map(g => `<div class="flow-group"><h3>${esc(ab("group")[g])}</h3>` +
        `<div class="flow-row">${COMPONENTS.filter(c => c.group === g).map(card).join("")}</div></div>`).join("")}</div>
    </div>
    <div class="panel walk">
      <div class="panel-h"><h2>${esc(ab("walk_title"))}</h2></div>
      <p class="panel-sub">${esc(ab("walk_hint"))}</p>
      <div class="walk-controls">
        ${["alert", "allow", "selected"].map(k => `<button type="button" data-walk="${k}" aria-pressed="${walk.choice === k}"` +
          `${candidates()[k] ? "" : " disabled"}>${esc(ab("w_" + k))}</button>`).join("")}
        <span class="grow"></span>
        <button type="button" class="primary" data-walk-play ${walk.rec ? "" : "disabled"}>${esc(ab(walk.timer ? "w_pause" : "w_play"))}</button>
      </div>
      <div id="walk-body"></div>
    </div>
    <div class="about-grid">
      <div class="panel">
        <div class="panel-h"><h2>${esc(ab("model_title"))}</h2><span class="grow"></span>
          <button type="button" data-features>${esc(fill(ab("features_btn"), {n: features}))}</button></div>
        <ul class="model-list">${ab("model_lines").map(line => `<li>${esc(fill(line, {
          fit: fmtInt(o.fit), val: fmtInt(o.val), test: fmtInt(o.test), tf: fmtInt(o.test_fraud),
          rec: pct(o.recall), prec: pct(o.precision), cut: dec(cut, 4), model_ms: fmtMs(modelMs)}))}</li>`).join("")}</ul>
      </div>
      <div class="panel">
        <div class="panel-h"><h2>${esc(ab("rules_title"))}</h2><span class="grow"></span>
          <button type="button" data-rules>${esc(fill(ab("rules_btn"), {n: Object.keys(RULE_FACTS).length}))}</button></div>
        <p class="panel-sub">${esc(ab("rules_intro"))}</p>
      </div>
    </div>`;
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
  openModal(`<span class="kind">${esc(kind)}</span><h3>${esc(name)}</h3>` +
    `<ul>${lines.map(line => `<li>${esc(fill(line, vals))}</li>`).join("")}</ul>`);
}

function openFeatures() {
  openModal(`<h3>${esc(ab("features_title"))}</h3><p class="small-note">${esc(ab("features_intro"))}</p>` +
    FEATURE_GROUPS.map(([g, names]) => `<h4 class="feat-group">${esc(ab("fgroup")[g])}</h4>` +
      `<ul class="feat-list">${names.map(n => `<li>${esc(ab("feature")[n])} <code>${esc(n)}</code></li>`).join("")}</ul>`).join(""), true);
}

function openRules() {
  openModal(`<h3>${esc(ab("rules_title"))}</h3><p class="small-note">${esc(ab("rules_intro"))}</p>` +
    `<div class="tbl"><table class="rules-table"><thead><tr><th>${esc(ab("r_rule"))}</th><th>${esc(ab("r_checks"))}</th>` +
    `<th>${esc(ab("r_does"))}</th></tr></thead><tbody>${Object.keys(RULE_FACTS).map(code =>
      `<tr><td>${esc(t("rules")[code] || code)}</td><td>${esc(ruleText(code))}</td><td>${roles(code)}</td></tr>`).join("")}` +
    `</tbody></table></div>`, true);
}

function stepText(step, r) {
  const alert = r.decision === "REVIEW";
  const key = step.key === "analyst" && !alert ? "analyst_allow" : step.key;
  const [title, text] = ab("walk")[key];
  const rules = r.rules || [], cut = status && status.cut;
  return [title, fill(text, {
    rules: rules.length ? fill(ab("fired"), {list: rules.map(x => t("rules")[x] || x).join(", ")}) : ab("none_fired"),
    score: dec(r.score, 2), decision: t("dec")[r.decision] || r.decision,
    why: rules.some(x => MUST.has(x)) ? ab("why_must")
      : fill(ab(alert ? "why_review" : "why_allow"), {score: dec(r.score, 2), cut: dec(cut, 2)}),
    alert: alert ? ab("alert_too") : ""})];
}

function paintWalk() {
  const body = $("#walk-body");
  if (!body) return;
  const r = walk.rec;
  if (!r) { body.innerHTML = `<p class="empty">${esc(ab("w_none"))}</p>`; return; }
  const cut = status && status.cut, rules = r.rules || [];
  const top = r.why && r.why.items && r.why.items[0];
  const reason = top ? (lang === "ru" ? (T.ru.feat[top.feature] || top.label_en) : top.label_en) : "";
  const facts = [
    [ab("c_risk"), `${dec(r.score, 2)} · ${fill(ab("c_level"), {cut: dec(cut, 2)})}`],
    [ab("c_rules"), rules.length ? rules.map(x => t("rules")[x] || x).join(", ") : ab("c_none")],
    ...(r.type ? [[ab("c_named"), kindName(r.type)]] : []),
    ...(reason ? [[ab("c_reason"), reason]] : []),
    [ab("c_total"), fmtMs(r.ms)]];
  const steps = WALK.map((s, i) => {
    const [title, text] = stepText(s, r);
    const ms = s.stage && r.stages && r.stages[s.stage] != null ? fmtMs(Number(r.stages[s.stage])) : "";
    const state = i === walk.idx ? "on" : i < walk.idx ? "past" : "";
    // Where it happens: the technology, or the service's name where that is just "Python".
    const [name, kind] = ab("parts")[s.at];
    return `<li class="tl ${state}" data-walk-at="${i}">` +
      `<span class="tl-dot"></span><div class="tl-head"><b>${esc(title)}</b>` +
      `<span class="tl-where">${esc(kind === "Python" ? name : kind)}</span><span class="tl-ms">${ms}</span></div>` +
      `<p class="tl-text">${esc(text)}</p></li>`;
  }).join("");
  body.innerHTML = `<div class="walk-grid">
    <div class="walk-card">
      <div class="case-top"><span class="amount">${fmtInt(r.amount)} <small>${t("sum")}</small></span>${badge(r.decision)}</div>
      <div class="route">${esc(r.from)} → ${esc(r.to)} · ${esc(fmtTime(r.at))}</div>
      <dl>${facts.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl>
    </div>
    <ol class="timeline">${steps}</ol>
  </div>`;
  document.querySelectorAll("#about .comp").forEach(el =>
    el.classList.toggle("on", walk.idx >= 0 && el.dataset.comp === WALK[walk.idx].at));
}

function walkStop() {
  clearInterval(walk.timer);
  walk.timer = null;
  const btn = document.querySelector("[data-walk-play]");
  if (btn) btn.textContent = ab("w_play");
}

function walkTo(i) {
  walk.idx = Math.max(0, Math.min(WALK.length - 1, i));
  paintWalk();
}

function walkPlay() {
  if (walk.timer) { walkStop(); return; }
  if (walk.idx >= WALK.length - 1) walk.idx = -1;
  walkTo(walk.idx + 1);
  walk.timer = setInterval(() => (walk.idx >= WALK.length - 1 ? walkStop() : walkTo(walk.idx + 1)), 1800);
  document.querySelector("[data-walk-play]").textContent = ab("w_pause");
}

document.addEventListener("click", e => {
  const el = e.target.closest("#about [data-comp],[data-walk],[data-walk-play],[data-walk-at],[data-features],[data-rules]");
  if (!el) return;
  if (el.dataset.comp) openComponent(el.dataset.comp);
  else if (el.hasAttribute("data-features")) openFeatures();
  else if (el.hasAttribute("data-rules")) openRules();
  else if (el.dataset.walk) {
    walkStop();
    walk.choice = el.dataset.walk;
    walk.rec = candidates()[walk.choice] || null;
    walk.idx = -1;
    drawAbout();
  }
  else if (el.hasAttribute("data-walk-play")) walkPlay();
  else if (el.dataset.walkAt) { walkStop(); walkTo(Number(el.dataset.walkAt)); }
});

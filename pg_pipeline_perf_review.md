Перепроверил: собрал старую и новую версии рядом, сверил диффы построчно и пересчитал все метрики из `samples/results` сам. Основное подтвердилось, но есть **одна реальная регрессия, которая замаскирована под «шум»**.

## Что подтвердилось (мои пересчёты, не твоя таблица)

| sample | alloc/op было → стало | ops/s |
|---|---|---|
| client_query | 13.00 → **8.00** | 4240 → 4829 |
| client_query_params | 20.00 → **12.00** | 4121 → 4964 |
| prepared_query | 18.00 → **10.00** | 4389 → 5314 |
| request_seal | 8.00 → **5.00** | 349k → 502k |
| native_dispatch | 5.62 → **3.48** | 4.14M → 4.18M |
| native_drain | 3.39 → 3.15, calls/batch 23.4 → **19.4** | 39k → 45k |
| result_rows | 524 → 519 | 2332 → 2748 |
| session_guard | 0 → 0 | 5597k → 5599k |

P0-1 закрыт полностью и это доказуемо: в seal-семпле harness сам аллоцирует `params_template.dup` + интерполяцию, а из двух параметров один уже frozen (`.map(&:freeze)`), второй — нет. То есть удалиться должно было ровно 3 объекта (snapshot + descriptors + один `rb_str_new_frozen`). Удалилось ровно 3. **Seal теперь стоит ноль Ruby-объектов.**

## Регрессия: `flush_calls_per_unit` в multiplex 0.17 → 1.0

```
было:  flush_calls: 151255 / units: 772784  = 0.196
       flush_calls: 168380 / units: 1060850 = 0.159
стало: flush_calls: 872498 / units: 872498  = 1.0
       flush_calls: 807362 / units: 807362  = 1.0
```

Это **5.25× больше `send()`-сисколлов** на тех же юнитах (по 9 байт каждый — `bytes_dispatched/units = 9`). 73k → 67k это не localhost-шум, это оно.

Причина — взаимодействие двух правок P0-2. Коалесинг решает флашить по условию `d.events.empty?`, но после того, как reader перестал слать `:readable`, а submit перестал ходить через owner, **очередь событий пуста почти всегда**. То есть `flush_now(d) if d.events.empty?` в inline-ветке срабатывает на каждом запросе. Эвристика «events непусты ⇒ работа ещё будет» была верна для owner-петли и стала бессмысленной для inline-пути: там сигнал «сейчас засабмитят ещё» — это другие готовые фиберы, а не очередь событий.

Минимальный фикс (одна строка, гарантированно возвращает 0.4.0-поведение на multiplex, сохраняя весь выигрыш single-fiber):

```ruby
def inline_dispatchable?(d)
  d.running && !d.draining &&
    d.submitting.zero? && d.dispatching.nil? &&
    !d.needs_flush && d.requests.empty? &&
    d.core.inflight_count.zero?      # было: < d.max_in_flight
end
```

Конвейер пуст ⇒ коалесить нечего, летим напрямую. Конвейер занят ⇒ идём через owner, который батчит как раньше.

Более сильный вариант (даёт и inline-dispatch, и коалесинг) — не флашить в inline-ветке, а отложить на owner с guard'ом:

```ruby
if ok
  d.flush_pending = true
  if d.core.inflight_count <= 1
    flush_now(d)          # мы одни в трубе, коалесить не с чем — латентность
  else
    notify_flush(d)       # один :flush на всю пачку
  end
end

def notify_flush(d)
  return if d.flush_event_pending || !d.running
  d.flush_event_pending = true
  d.events.enqueue(:flush)
end
```
`process_event`: `when :flush then d.flush_event_pending = false`. И тогда обязательно добавить `!d.flush_pending` в `drained?` — иначе graceful close может завершиться с неотправленными байтами.

## Bench suite не перепрогнан

В `samples/results` нет `all_benchmarks.txt` — только 9 профилей. `run_all_profiles.sh` бенчи не гоняет. А коалесинг флашей делался ровно ради `throughput`/`ab` (2000 фиберов на 4 соединения, где было `flush/unit 0.99`, `in_flight_peak 5`) — и по multiplex-данным там сейчас скорее всего тоже 1.0. Плюс `metrics` — единственный источник для `allocations/query` и профиля Ruby control plane. Прогони `rake bench` до и после фикса.

## Корректность — четыре замечания

1. **Reentrancy inline-dispatch.** В `bounded_queue.c` у тебя есть комментарий, что планировщик вправе резюмировать фибер прямо внутри `unblock`. Теперь `consume_and_drain` вызывается из reader'а, и такой inline-resume разбуженного caller'а приведёт его в `submit` → `inline_dispatchable?` → `PQsendQueryParams` **посреди** `pp_driver_drain_body`. Раньше caller в этой ситуации мог только положить запрос в очередь. Лечится дёшево: флаг вокруг дренажа в reader'е и `!d.reader_draining` в `inline_dispatchable?`.

2. **Изменилась поверхность ошибок.** Раньше `submit` не мог бросить `ConnectionLostError` — dispatch был асинхронным, и пользователь получал `NotDispatchedError`/`IndeterminateResultError` из `wait`. Теперь inline-ветка делает `fatal_close` и `raise`, а `submit_with_failover` ловит только `NotDispatchedError`/`ShutdownError` — то есть **failover на другой драйвер не сработает**, и наружу полетит другой класс исключения. Либо транслировать в `NotDispatchedError` (тогда failover заработает, что даже лучше прежнего), либо явно задокументировать.

3. **`drained?` не учитывает `flush_pending`** (сейчас безвредно, потому что флаг всегда сбрасывается сразу; станет багом с отложенным флашем — см. выше).

4. Мелочи: `bq_next_pow2` при переполнении возвращает `n`, и тогда `mask = n-1` некорректна для не-степени двойки — лучше `rb_raise`, чем молчаливо битая маска (недостижимо на практике, но это тихий путь). В `pp_result_each` `ALLOCV_END` пропускается при `break`/исключении из блока — не течёт (буфер GC-managed), но при `nfields > 16` чище обернуть в `rb_ensure`.

Отдельно хорошо сделано: `limit` и `capacity` в BoundedQueue разведены, так что округление до степени двойки не сломало backpressure. Это была самая вероятная ловушка в той правке, и ты её обошёл.

## Тестов на новую модель конкурентности нет

`spec/`, `bench_kit/` и `samples/*.rb` побайтово идентичны предыдущей версии. 325 зелёных примеров — это тот же набор, что проходил на старой архитектуре. Три самые рискованные правки (inline dispatch, reader-drain, коалесинг) не покрыты ничем. Минимум, что стоит добавить: FIFO при переходе inline→queued под забитым `in_flight`; `graceful_close` с непрофлашенными байтами; `ConnectionLostError` из inline-submit и что происходит с `respawn`; дренаж, во время которого разбуженный фибер сабмитит новый запрос.

**Порядок:** сначала однострочный фикс `inline_dispatchable?`, перепрогон multiplex (ждём возврат к ~73k и `flush/unit ≈ 0.16`), затем `rake bench` для `throughput`/`ab`/`metrics`, и только потом — отложенный флаш через `:flush`-событие, если хочешь выжать батчинг ещё и из inline-пути.

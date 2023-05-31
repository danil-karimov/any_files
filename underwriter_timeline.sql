with appsByStatus as (
    select id,
           arrayFirst(id, roles -> has(roles, 'BORROWER'), `participants.id`, `participants.roles`)                                       as borrowerId,
           modifiedAt as startTime,
           status,
           version,
           neighbor(id, -1) as pid,
           neighbor(status, -1) as pstatus,
           workers['LAWYER'] as workerId,
           dictGetString('analytics-service.office_users_dict', 'fullName', workerId) as workerName
    from `analytics-service`.applications_v1
    where pstatus != status
       or pid != id
    order by id, version asc),
    
     appsWithStartAndEndTime as (
         select *,
                neighbor(id, 1) as nid,
                if(id = nid, neighbor(startTime, 1), startTime) as endTime,
                date_diff('minute', startTime, endTime) as statusTime
         from appsByStatus
         order by id, version asc
     ),
     appsWithWorkingTime as (
         select id,
                --city,
                fullName as borrower,
                status,
                startTime,
                endTime,
                min2(workTimeTemp, statusTime) as workTime,
                statusTime - workTime as nonWorkTime,
                statusTime,
                workerName,
                parseDateTimeBestEffort(formatDateTime(startTime, '%Y-%m-%dT03:00:00.000Z')) as startWorkDay,
                parseDateTimeBestEffort(formatDateTime(endTime, '%Y-%m-%dT12:59:00.000Z')) as endWorkDay,
                600 as workDayInMin,
                date_diff('day', startTime, endTime) as workDaysCount,
                (workDaysCount + 1) * workDayInMin
                    - min2(max2(date_diff('minute', startWorkDay, startTime), 0), workDayInMin)
                    - min2(max2(date_diff('minute', endTime, endWorkDay), 0), workDayInMin)  as workTimeTemp,
                version
         from appsWithStartAndEndTime l
                  left join (select id, fullName
                             from `analytics-service`.persons_v1
                             order by version desc
                             limit 1 by id) p
                            on p.id = l.borrowerId
     ),
     sumWorkingTimeByStatus as (
         select id,
                --concat('https://office.credit.club/lead/', id) as `Ссылка`,
                --any(borrower) as `Заемщик`,
                --any(city) as `Город`,
                status as `Статус`,
                min(startTime) as `Время поступления в статус`,
                sum(workTime) as `Рабочее время в статусе`,
                sum(nonWorkTime) as `Нерабочее время в статусе`,
                sum(statusTime)  as `Общее время в статусе`,
                any(workerName)  as `ФИО менежера`
         from appsWithWorkingTime
         group by id, status
         order by id, min(version)
     )
select *,
       sum(`Рабочее время в статусе`) over (partition by id)   as `Время работы с заявкой в рабочее время`,
       sum(`Нерабочее время в статусе`) over (partition by id) as `Время работа с заявкой в нерабочее время`,
       sum(`Общее время в статусе`) over (partition by id)     as `Общее время работы с заявкой`
from sumWorkingTimeByStatus
where `Время поступления в статус` >= '2023-01-01'








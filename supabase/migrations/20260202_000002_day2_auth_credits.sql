-- Day 2: Auth profiles + credits + user_jobs (separate from worker jobs tables)
-- This avoids conflicts with existing public.jobs and public.clip_jobs.

-- 1) Profiles table
create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text,
  credits integer not null default 10,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- 2) Auto-create profile on signup
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.profiles (id, email, credits)
  values (new.id, new.email, 10)
  on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
after insert on auth.users
for each row execute procedure public.handle_new_user();

-- 3) user_jobs table (owned by user, references worker_job_id stored in worker/clip_jobs)
create table if not exists public.user_jobs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  youtube_url text not null,
  worker_job_id uuid not null,
  status text not null default 'submitted',
  credits_spent integer not null default 1,
  created_at timestamptz not null default now()
);

create index if not exists user_jobs_user_id_idx on public.user_jobs(user_id);
create index if not exists user_jobs_worker_job_id_idx on public.user_jobs(worker_job_id);

-- 4) RLS
alter table public.profiles enable row level security;
alter table public.user_jobs enable row level security;

-- Profiles policies
drop policy if exists "profiles_select_own" on public.profiles;
create policy "profiles_select_own"
on public.profiles
for select
to authenticated
using (id = auth.uid());

drop policy if exists "profiles_update_own" on public.profiles;
create policy "profiles_update_own"
on public.profiles
for update
to authenticated
using (id = auth.uid())
with check (id = auth.uid());

-- user_jobs policies
drop policy if exists "user_jobs_select_own" on public.user_jobs;
create policy "user_jobs_select_own"
on public.user_jobs
for select
to authenticated
using (user_id = auth.uid());

drop policy if exists "user_jobs_insert_own" on public.user_jobs;
create policy "user_jobs_insert_own"
on public.user_jobs
for insert
to authenticated
with check (user_id = auth.uid());

-- 5) Credits: reserve and refund (atomic)
create or replace function public.reserve_credits(amount integer)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  uid uuid := auth.uid();
  remaining integer;
begin
  if uid is null then
    raise exception 'not authenticated';
  end if;

  update public.profiles
  set credits = credits - amount,
      updated_at = now()
  where id = uid
    and credits >= amount
  returning credits into remaining;

  if remaining is null then
    raise exception 'insufficient credits';
  end if;

  return remaining;
end;
$$;

create or replace function public.refund_credits(amount integer)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  uid uuid := auth.uid();
  remaining integer;
begin
  if uid is null then
    raise exception 'not authenticated';
  end if;

  update public.profiles
  set credits = credits + amount,
      updated_at = now()
  where id = uid
  returning credits into remaining;

  if remaining is null then
    raise exception 'profile missing';
  end if;

  return remaining;
end;
$$;

revoke all on function public.reserve_credits(integer) from public;
revoke all on function public.refund_credits(integer) from public;
grant execute on function public.reserve_credits(integer) to authenticated;
grant execute on function public.refund_credits(integer) to authenticated;

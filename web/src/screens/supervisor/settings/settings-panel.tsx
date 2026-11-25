// The stand settings of decision 0014 on the supervisor screen. Shown only
// when the gateway sent a settings block: an older gateway gets no panel at
// all. Collapsed by default; the form keeps its edits while collapsed.

import { useId, useReducer, useRef, useState, type ChangeEvent, type ReactNode } from 'react';

import { useTickMs } from '../../../live/clock.ts';
import { useLive, useSession } from '../../../live/hooks.ts';
import type { SettingsBlock } from '../../../protocol/types.ts';
import { Button } from '../../../ui/button.tsx';
import { HelpHint } from '../../../ui/help-hint.tsx';
import { ChevronDownIcon } from '../../../ui/icons.tsx';
import { ChangesList } from './changes-list.tsx';
import { SPEED_TEXT, boundOf, formatSetting, inputUnit, type FieldMeta } from './fields.ts';
import { INITIAL_FORM, answerText, deriveForm, formReducer, isErrorAnswer, retrySecondsLeft, type FieldView, type FormState, type FormView } from './form.ts';

export function SettingsPanel() {
  const block = useLive((s) => s.settings);
  if (block === null) return null;
  return <SettingsPanelBody block={block} />;
}

function SettingsPanelBody({ block }: { readonly block: SettingsBlock }) {
  const session = useSession();
  const [state, dispatch] = useReducer(formReducer, INITIAL_FORM);
  const [isOpen, setIsOpen] = useState(false);
  const retryTimer = useRef<number | undefined>(undefined);
  const headingId = useId();
  const bodyId = useId();

  const view = deriveForm(state, block);
  const canEdit = block.may_change;

  function handleToggle() {
    setIsOpen((open) => !open);
  }

  function handleEdit(meta: FieldMeta, text: string) {
    dispatch({ type: 'edit', field: meta.field, text, version: block.version, isClean: view.editedCount === 0 });
  }

  function handleDiscard() {
    dispatch({ type: 'discard' });
  }

  function handleReset() {
    dispatch({ type: 'reset_to_defaults', block });
  }

  function handleKeepEdits() {
    dispatch({ type: 'keep_edits', version: block.version });
  }

  async function handleApply() {
    if (!view.canApply || view.next === null) return;
    dispatch({ type: 'submit' });
    const result = await session.changeSettings(block.version, view.next);
    // The settings message of a done change came before the result.
    const version = session.store.getState().settings?.version ?? block.version;
    dispatch({ type: 'answered', result, version, nowMs: Date.now() });
    if (result.code === 'settings_too_soon') {
      window.clearTimeout(retryTimer.current);
      retryTimer.current = window.setTimeout(() => dispatch({ type: 'retry_ready' }), result.retry_after_ms ?? 1000);
    }
  }

  const summary = [
    `version ${block.version}`,
    canEdit ? null : 'read only',
    view.editedCount > 0 ? `${view.editedCount} unsaved ${view.editedCount === 1 ? 'change' : 'changes'}` : null,
  ].filter((p) => p !== null);

  return (
    <section aria-labelledby={headingId} className="min-w-0 rounded-lg border border-line bg-panel">
      <header className={`flex min-h-11 flex-wrap items-center gap-x-3 gap-y-1 px-4 py-2 ${isOpen ? 'border-b border-line' : ''}`}>
        <div className="flex items-center gap-1.5">
          <h2 id={headingId} className="eyebrow">
            <button
              type="button"
              aria-expanded={isOpen}
              aria-controls={bodyId}
              onClick={handleToggle}
              className="-mx-1 inline-flex items-center gap-1.5 rounded px-1 py-1 uppercase hover:text-ink"
            >
              <ChevronDownIcon className={`size-3.5 transition-transform ${isOpen ? '' : '-rotate-90'}`} />
              Stand settings
            </button>
          </h2>
          <HelpHint id="supervisor.panel.settings" label="Stand settings" />
        </div>
        <div className="text-xs text-ink-3">{summary.join(', ')}</div>
      </header>
      <div id={bodyId} hidden={!isOpen} className="flex flex-col gap-4 p-4">
        {!canEdit && (
          <p className="text-sm text-ink-2">
            These are the values the stand uses now. Changing them needs a site account with access to the stand settings.
          </p>
        )}
        {view.isOutdated && <OutdatedNote version={block.version} onKeep={handleKeepEdits} onDiscard={handleDiscard} />}
        <div className="grid gap-4 lg:grid-cols-2">
          <FieldGroup title="Simulated operators" view={view} prefix="sim." canEdit={canEdit} block={block} onEdit={handleEdit}>
            {!view.hasSim && (
              <p className="py-3 text-sm text-ink-3">Not available: this stand does not offer simulator settings.</p>
            )}
          </FieldGroup>
          <FieldGroup title="Dedicated teams" view={view} prefix="teams." canEdit={canEdit} block={block} onEdit={handleEdit} />
        </div>
        {view.simError !== null && <p className="text-sm text-late">Simulated operators: {view.simError}</p>}
        {canEdit && (
          <div className="flex flex-wrap items-center gap-3 border-t border-line pt-4">
            <AnswerLine state={state} />
            <div className="ml-auto flex flex-wrap items-center gap-2">
              <Button size="sm" tone="quiet" onClick={handleDiscard} disabled={Object.keys(state.drafts).length === 0 || state.isPending}>
                Discard changes
              </Button>
              <Button size="sm" onClick={handleReset} disabled={!view.canReset}>
                Reset to defaults
              </Button>
              <Button size="sm" tone="primary" onClick={() => void handleApply()} disabled={!view.canApply}>
                {state.isPending ? 'Applying' : 'Apply'}
              </Button>
            </div>
          </div>
        )}
        <ChangesList block={block} />
      </div>
    </section>
  );
}

interface FieldGroupProps {
  readonly title: string;
  readonly view: FormView;
  readonly prefix: 'sim.' | 'teams.';
  readonly canEdit: boolean;
  readonly block: SettingsBlock;
  readonly onEdit: (meta: FieldMeta, text: string) => void;
  readonly children?: ReactNode;
}

function FieldGroup({ title, view, prefix, canEdit, block, onEdit, children }: FieldGroupProps) {
  const fields = view.fields.filter((f) => f.meta.field.startsWith(prefix));
  return (
    <fieldset className="min-w-0 rounded-md border border-line px-4 pb-1">
      <legend className="eyebrow px-1">{title}</legend>
      {children}
      <ul className="divide-y divide-line">
        {fields.map((f) => (
          <li key={f.meta.field}>
            <FieldRow field={f} canEdit={canEdit} block={block} onEdit={onEdit} />
          </li>
        ))}
      </ul>
    </fieldset>
  );
}

interface FieldRowProps {
  readonly field: FieldView;
  readonly canEdit: boolean;
  readonly block: SettingsBlock;
  readonly onEdit: (meta: FieldMeta, text: string) => void;
}

function FieldRow({ field, canEdit, block, onEdit }: FieldRowProps) {
  const inputId = useId();
  const noteId = useId();
  const errorId = useId();
  const { meta } = field;
  const bound = boundOf(block.bounds, meta.field);
  const unit = inputUnit(meta, bound);
  const inUse = formatSetting(meta.field, field.current, bound);
  const byDefault = field.defaultValue === undefined ? null : formatSetting(meta.field, field.defaultValue, bound);

  function handleChange(event: ChangeEvent<HTMLInputElement>) {
    onEdit(meta, event.target.value);
  }

  return (
    <div className="flex flex-col gap-2 py-3 sm:grid sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center sm:gap-x-4 sm:gap-y-1">
      <div className="min-w-0">
        <span className="inline-flex items-center gap-1.5 text-sm">
          {canEdit ? <label htmlFor={inputId}>{meta.label}</label> : <span>{meta.label}</span>}
          <HelpHint id={meta.hint} label={meta.label} />
        </span>
        <p id={noteId} className="text-xs text-ink-3">
          {canEdit ? `In use ${inUse}` : null}
          {canEdit && byDefault !== null ? ', ' : null}
          {byDefault !== null ? `${canEdit ? 'default' : 'Default'} ${byDefault}` : null}. {capitalize(SPEED_TEXT[meta.speed])}.
        </p>
      </div>
      {canEdit ? (
        <span className="flex items-center gap-2">
          <input
            id={inputId}
            type="text"
            inputMode="decimal"
            autoComplete="off"
            spellCheck={false}
            value={field.text}
            onChange={handleChange}
            aria-invalid={field.error !== null}
            aria-describedby={field.error === null ? noteId : `${errorId} ${noteId}`}
            className={`h-8 w-20 rounded-md border bg-panel px-2 text-right font-mono text-sm tnum focus-visible:outline-offset-[-1px] ${
              field.error !== null ? 'border-late' : field.isEdited ? 'border-accent bg-accent-soft' : 'border-line-strong'
            }`}
          />
          <span className="text-xs text-ink-3 sm:w-24">{unit}</span>
        </span>
      ) : (
        <span className="font-mono text-sm tnum sm:text-right">{inUse}</span>
      )}
      {field.error !== null && (
        <p id={errorId} className="text-xs text-late sm:col-span-2">
          {field.error}
        </p>
      )}
    </div>
  );
}

interface OutdatedNoteProps {
  readonly version: number;
  readonly onKeep: () => void;
  readonly onDiscard: () => void;
}

function OutdatedNote({ version, onKeep, onDiscard }: OutdatedNoteProps) {
  return (
    <div role="alert" className="flex flex-wrap items-center gap-x-4 gap-y-2 rounded-md border border-wait/40 bg-wait-soft px-4 py-3 text-sm">
      <p className="min-w-0 flex-1">
        <strong className="font-semibold">Someone else changed the settings (now version {version}).</strong>{' '}
        <span className="text-ink-2">
          Fields you did not edit show the new values. Your edits are kept but not applied: keep them on top of the new values, or discard them.
        </span>
      </p>
      <div className="flex gap-2">
        <Button size="sm" onClick={onKeep}>
          Keep my edits
        </Button>
        <Button size="sm" tone="quiet" onClick={onDiscard}>
          Discard my edits
        </Button>
      </div>
    </div>
  );
}

function AnswerLine({ state }: { readonly state: Pick<FormState, 'answer' | 'retryAtMs' | 'retryWaitMs'> }) {
  return (
    <p role="status" className="min-w-0 flex-1 text-sm empty:hidden">
      {state.answer !== null &&
        (state.answer.kind === 'too_soon' && state.retryAtMs !== null && state.retryWaitMs !== null ? (
          <RetryText retryAtMs={state.retryAtMs} waitMs={state.retryWaitMs} />
        ) : (
          <span className={isErrorAnswer(state.answer) ? 'text-late' : 'text-done'}>{answerText(state.answer, 0)}</span>
        ))}
    </p>
  );
}

/** The "too soon" answer with the seconds left; it reads the shared clock only while shown. */
function RetryText({ retryAtMs, waitMs }: { readonly retryAtMs: number; readonly waitMs: number }) {
  const nowMs = useTickMs();
  const seconds = retrySecondsLeft(retryAtMs, waitMs, nowMs);
  return <span className="text-late">{answerText({ kind: 'too_soon' }, seconds)}</span>;
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

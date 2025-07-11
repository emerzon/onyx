import { DateRangePickerValue } from "@/components/dateRangeSelectors/AdminDateRangeSelector";
import { Tag, ValidSources } from "../types";
import { Persona } from "@/app/admin/assistants/interfaces";

export const FlowType = {
  SEARCH: "search",
  QUESTION_ANSWER: "question-answer",
};
export type FlowType = (typeof FlowType)[keyof typeof FlowType];
export const SearchType = {
  SEMANTIC: "semantic",
  KEYWORD: "keyword",
  AUTOMATIC: "automatic",
  INTERNET: "internet",
};
export type SearchType = (typeof SearchType)[keyof typeof SearchType];

export interface ProSearchPacket {
  sub_question?: string;
  answer_piece?: string;
  sub_query?: string;
  tool_response?: ToolResponse;
  level: number;
  level_question_num: number;
}

export interface RefinedAnswerImprovement {
  refined_answer_improvement: boolean;
}

export interface AgentAnswerPiece {
  answer_piece: string;
  level: number;
  level_question_num: number;
  answer_type: "agent_sub_answer" | "agent_level_answer";
}

export interface SubQuestionPiece {
  sub_question: string;
  level: number;
  level_question_num: number;
}

export interface SubQueryPiece {
  sub_query: string;
  level: number;
  level_question_num: number;
  query_id: number;
}

export interface SubQuestionSearchDoc {
  context_docs: OnyxDocument[];
  level_question_num: number;
  level: number;
}

export interface ToolResponse {
  id?: string | null;
  response?: any;
}
export interface ExtendedToolResponse extends ToolResponse {
  level: number;
  level_question_num: number;
}

export interface AnswerPiecePacket {
  answer_piece: string;
}

export enum StreamStopReason {
  CONTEXT_LENGTH = "CONTEXT_LENGTH",
  CANCELLED = "CANCELLED",
}

export interface StreamStopInfo {
  stop_reason: StreamStopReason;
  level?: number;
  level_question_num?: number;
  stream_type?: "sub_answer" | "sub_questions" | "main_answer";
}

export interface ErrorMessagePacket {
  error: string;
}

export interface Quote {
  quote: string;
  document_id: string;
  link: string | null;
  source_type: ValidSources;
  blurb: string;
  semantic_identifier: string;
}

export interface QuotesInfoPacket {
  quotes: Quote[];
}
export interface MinimalOnyxDocument {
  document_id: string;
  semantic_identifier: string | null;
}

export interface OnyxDocument extends MinimalOnyxDocument {
  link: string;
  source_type: ValidSources;
  blurb: string;
  boost: number;
  hidden: boolean;
  score: number;
  chunk_ind: number;
  match_highlights: string[];
  metadata: { [key: string]: string };
  updated_at: string | null;
  db_doc_id?: number;
  is_internet: boolean;
  validationState?: null | "good" | "bad";
}

export interface LoadedOnyxDocument extends OnyxDocument {
  icon: React.FC<{ size?: number; className?: string }>;
}

export interface SearchOnyxDocument extends OnyxDocument {
  is_relevant: boolean;
  relevance_explanation: string;
}

export interface FilteredOnyxDocument extends OnyxDocument {
  included: boolean;
}
export interface DocumentInfoPacket {
  top_documents: OnyxDocument[];
  predicted_flow: FlowType | null;
  predicted_search: SearchType | null;
  time_cutoff: string | null;
  favor_recent: boolean;
}

export interface DocumentRelevance {
  relevant: boolean;
  content: string;
}

export interface Relevance {
  [url: string]: DocumentRelevance;
}

export interface RelevanceChunk {
  relevance_summaries: Relevance;
}

export interface SearchResponse {
  suggestedSearchType: SearchType | null;
  suggestedFlowType: FlowType | null;
  answer: string | null;
  quotes: Quote[] | null;
  documents: SearchOnyxDocument[] | null;
  selectedDocIndices: number[] | null;
  error: string | null;
  messageId: number | null;
  additional_relevance?: Relevance;
}

export enum SourceCategory {
  Storage = "Storage",
  Wiki = "Wiki",
  CustomerSupport = "Customer Support",
  CustomerRelationshipManagement = "Customer Relationship Management",
  Messaging = "Messaging",
  ProjectManagement = "Project Management",
  CodeRepository = "Code Repository",
  Other = "Other",
}

export interface SourceMetadata {
  icon: React.FC<{ size?: number; className?: string }>;
  displayName: string;
  category: SourceCategory;
  shortDescription?: string;
  internalName: ValidSources;
  adminUrl: string;
  oauthSupported?: boolean;
  federated?: boolean;
  federatedTooltip?: string;
  // For federated connectors, this stores the base source type for the icon
  baseSourceType?: ValidSources;
}

export interface SearchDefaultOverrides {
  forceDisplayQA: boolean;
  offset: number;
}

export interface Filters {
  source_type: string[] | null;
  document_set: string[] | null;
  time_cutoff: Date | null;
  user_file_ids: number[] | null;
  // user_folder_ids: number[] | null;
}

export interface SearchRequestArgs {
  query: string;
  agentic?: boolean;
  sources: SourceMetadata[];
  documentSets: string[];
  timeRange: DateRangePickerValue | null;
  tags: Tag[];
  persona: Persona;
  updateDocumentRelevance: (relevance: any) => void;
  updateCurrentAnswer: (val: string) => void;
  updateQuotes: (quotes: Quote[]) => void;
  updateDocs: (documents: OnyxDocument[]) => void;
  updateSelectedDocIndices: (docIndices: number[]) => void;
  updateSuggestedSearchType: (searchType: SearchType) => void;
  updateSuggestedFlowType: (flowType: FlowType) => void;
  updateError: (error: string) => void;
  updateMessageAndThreadId: (
    messageId: number,
    chat_session_id: string
  ) => void;
  finishedSearching: () => void;
  updateComments: (comments: any) => void;
  selectedSearchType: SearchType | null;
}

export interface SearchRequestOverrides {
  searchType?: SearchType;
  offset?: number;
  overrideMessage?: string;
  agentic?: boolean;
}

export interface ValidQuestionResponse {
  reasoning: string | null;
  error: string | null;
}

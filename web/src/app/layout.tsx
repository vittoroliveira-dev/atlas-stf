import type { Metadata, Viewport } from "next";
import { IBM_Plex_Sans, IBM_Plex_Mono } from "next/font/google";
import { TopBar } from "@/components/dashboard/top-bar";
import "./globals.css";

const plexSans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-plex-sans",
  display: "swap",
});

const plexMono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-plex-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Atlas STF · Painel Analítico",
  description: "Painel para entender períodos, casos e pontos de atenção do Atlas STF com linguagem mais clara e leitura guiada.",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: "#007D30",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="pt-BR" className={`${plexSans.variable} ${plexMono.variable}`}>
      <body className="font-sans antialiased">
        <a
          href="#main-content"
          className="sr-only focus-visible:not-sr-only focus-visible:fixed focus-visible:left-4 focus-visible:top-4 focus-visible:z-50 focus-visible:inline-flex focus-visible:h-11 focus-visible:items-center focus-visible:rounded-lg focus-visible:bg-verde-700 focus-visible:px-4 focus-visible:text-sm focus-visible:font-semibold focus-visible:text-white focus-visible:shadow-elevation-2"
        >
          Pular para o conteúdo
        </a>
        <TopBar />
        {children}
      </body>
    </html>
  );
}
